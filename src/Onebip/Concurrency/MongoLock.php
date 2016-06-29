<?php
namespace Onebip\Concurrency;

use DateInterval;
use DateTime;
use DateTimeZone;
use MongoDB;
use Onebip\Clock;
use Onebip\Clock\SystemClock;
use Onebip\DateTime\UTCDateTime;

class MongoLock implements Lock
{
    private $collection;
    private $processName;
    const DUPLICATE_KEY = 11000;

    public function __construct(MongoDB\Collection $collection, $programName, $processName, $clock = null, $sleep = 'sleep')
    {
        $this->collection = $collection;
        $this->collection->createIndex(['program' => 1], ['unique' => true]);
        $this->programName = $programName;
        $this->processName = $processName;
        if ($clock === null) {
            $clock = new SystemClock();
        }
        $this->clock = $clock;
        $this->sleep = $sleep;
    }

    public static function forProgram($programName, MongoDB\Collection $collection)
    {
        return new self($collection, $programName, gethostname() . ':' . getmypid());
    }

    public function acquire($duration = 3600)
    {
        $now = $this->clock->current();

        $this->removeExpiredLocks($now);

        $expiration = clone $now;
        $expiration->add(new DateInterval("PT{$duration}S"));

        try {
            $this->collection->insertOne([
                'program' => $this->programName,
                'process' => $this->processName,
                'acquired_at' => $this->dateTimeToMongo($now),
                'expires_at' => $this->dateTimeToMongo($expiration),
            ]);
        } catch (MongoDB\Driver\Exception\BulkWriteException $e) {
            if (strpos($e->getMessage(), 'E11000') === 0) {
                throw new LockNotAvailableException(
                    "{$this->processName} cannot acquire a lock for the program {$this->programName}"
                );
            }
            throw $e;
        }
    }

    public function refresh($duration = 3600)
    {
        $now = $this->clock->current();

        $this->removeExpiredLocks($now);

        $expiration = clone $now;
        $expiration->add(new DateInterval("PT{$duration}S"));

        $result = $this->collection->updateOne(
            ['program' => $this->programName, 'process' => $this->processName],
            ['$set' => ['expires_at' => $this->dateTimeToMongo($expiration)]]
        );

        if (!$this->lockRefreshed($result)) {
            throw new LockNotAvailableException(
                "{$this->processName} cannot acquire a lock for the program {$this->programName}"
            );
        }
    }

    public function show()
    {
        $document = $this->collection->findOne(
            ['program' => $this->programName],
            ['typeMap' => ['root' => 'array']]
        );
        if (!is_null($document)) {
            $document['acquired_at'] =
                $this->convertToIso8601String($document['acquired_at']);
            $document['expires_at'] =
                $this->convertToIso8601String($document['expires_at']);
            unset($document['_id']);
        }
        return $document;
    }

    public function release($force = false)
    {
        $query = ['program' => $this->programName];
        if (!$force) {
            $query['process'] = $this->processName;
        }
        $operationResult = $this->collection->deleteMany($query);

        if ($operationResult->getDeletedCount() !== 1) {
            throw new LockNotAvailableException(
                "{$this->processName} does not have a lock for {$this->programName} to release"
            );
        }
    }

    /**
     * @param integer $polling  how frequently to check the lock presence
     * @param integer $maximumWaitingTime  a limit to the waiting
     */
    public function wait($polling = 30, $maximumWaitingTime = 3600)
    {
        $timeLimit = $this->clock->current()->add(new DateInterval("PT{$maximumWaitingTime}S"));
        while (true) {
            $now = $this->clock->current();
            $result = $this->collection->count([
                'program' => $this->programName,
                'expires_at' => ['$gte' => $this->dateTimeToMongo($now)],
            ]);

            if ($result) {
                if ($now > $timeLimit) {
                    throw new LockNotAvailableException(
                        "I have been waiting up until {$timeLimit->format(DateTime::ISO8601)} for the lock $this->programName ($maximumWaitingTime seconds polling every $polling seconds), but it is still not available (now is {$now->format(DateTime::ISO8601)})."
                    );
                }
                call_user_func($this->sleep, $polling);
            } else {
                break;
            }
        }
    }

    private function removeExpiredLocks(DateTime $now)
    {
        $this->collection->deleteMany([
            'program' => $this->programName,
            'expires_at' => [
                '$lt' => $this->dateTimeToMongo($now),
            ],
        ]);
    }

    private function convertToIso8601String(MongoDB\BSON\UTCDateTime $dateTime)
    {
        return UTCDateTime::box($dateTime)->toIso8601();
    }

    private function lockRefreshed($result)
    {
        if ($result->getModifiedCount() === 1) {
            return true;
        }

        // result is not known (write concern is not set) so we check to see if
        // a lock document exists, if lock document exists we are pretty sure
        // that its update succeded
        return !is_null($this->show());
    }

    private function dateTimeToMongo(DateTime $dateTime)
    {
        return UTCDateTime::box($dateTime)->toMongoUTCDateTime();
    }
}
