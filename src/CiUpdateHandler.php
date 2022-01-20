<?php

namespace IcingaMetrics;

use Closure;
use gipfl\IcingaPerfData\Ci;
use gipfl\Json\JsonString;
use gipfl\ReactUtils\RetryUnless;
use gipfl\RedisUtils\RedisUtil;
use gipfl\RrdTool\DsList;
use gipfl\RrdTool\RraSet;
use Psr\Log\LoggerInterface;
use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidInterface;
use React\EventLoop\Loop;
use React\Promise\ExtendedPromiseInterface;
use function React\Promise\Timer\sleep;

/*

Update looks like this:
1) 1) "rrd:ci-changes"
   2)  1) 1) "1638971400710-0"
          2) 1) "config"
             2) "{\"uuid\":\"4bd7d8ce-34b4-4241-9ac3-626ecfca02c7\",\"filename\":\"4b/d7/4bd7d8ce34b442419ac3626ecfca02c
                7.rrd\",\"dsNames\":[\"irq\",\"user\",\"softirq\",\"system\",\"guest\",\"iowait\",\"guest_nice\",\"steal
                \",\"idle\",\"nice\"],\"dsMap\":{\"irq\":\"irq\",\"user\":\"user\",\"softirq\":\"softirq\",\"system\":\"
                system\",\"guest\":\"guest\",\"iowait\":\"iowait\",\"guest_nice\":\"guest_nice\",\"steal\":\"steal\",\"i
                dle\":\"idle\",\"nice\":\"nice\"}}"
             3) "ds"
             4) "DS:irq:COUNTER:8640:U:U DS:user:COUNTER:8640:U:U DS:softirq:COUNTER:8640:U:U DS:system:COUNTER:8640:U:U
                 DS:guest:COUNTER:8640:U:U DS:iowait:COUNTER:8640:U:U DS:guest_nice:COUNTER:8640:U:U DS:steal:COUNTER:86
                40:U:U DS:idle:COUNTER:8640:U:U DS:nice:COUNTER:8640:U:U"
             5) "rra"
             6) "RRA:AVERAGE:0.5:1:2880 RRA:AVERAGE:0.5:5:2880 RRA:MAX:0.5:1:2880 RRA:MAX:0.5:5:2880 RRA:MIN:0.5:1:2880
                RRA:MIN:0.5:5:2880"
       2) 1) "1638971400713-0"
          2) 1) "config"
             2) "{\"uuid\":\"dea4a4d4-855c-4ea7-99a1-485b566fdf4e\",\"filename\":\"de/a4/dea4a4d4855c4ea799a1485b566fdf4
                e.rrd\",\"dsNames\":[\"invalid\",\"deferred\",\"succeeded\"],\"dsMap\":{\"invalid\":\"invalid\",\"deferr
                ed\":\"deferred\",\"succeeded\":\"succeeded\"}}"
             3) "ds"
             4) "DS:invalid:COUNTER:8640:U:U DS:deferred:COUNTER:8640:U:U DS:succeeded:COUNTER:8640:U:U"
             5) "rra"
             6) "RRA:AVERAGE:0.5:1:2880 RRA:AVERAGE:0.5:5:2880 RRA:MAX:0.5:1:2880 RRA:MAX:0.5:5:2880 RRA:MIN:0.5:1:2880
                RRA:MIN:0.5:5:2880"
       3) 1) "1638971400716-0"
          2) 1) "config"
             2) "{\"uuid\":\"c2831da3-108e-4bea-bea0-9271bc24ce2c\",\"filename\":\"c2/83/c2831da3108e4beabea09271bc24ce2
                c.rrd\",\"dsNames\":[\"txDrop\",\"txColls\",\"rxFifo\",\"txCarrier\",\"txCompressed\",\"rxFrame\",\"rxDr
                op\",\"txFifo\",\"rxMulticast\",\"txPackets\",\"rxErrs\",\"rxPackets\",\"rxBytes\",\"txErrs\",\"txBytes
                \",\"rxCompressed\"],\"dsMap\":{\"txDrop\":\"txDrop\",\"txColls\":\"txColls\",\"rxFifo\":\"rxFifo\",\"tx
                Carrier\":\"txCarrier\",\"txCompressed\":\"txCompressed\",\"rxFrame\":\"rxFrame\",\"rxDrop\":\"rxDrop\",
                \"txFifo\":\"txFifo\",\"rxMulticast\":\"rxMulticast\",\"txPackets\":\"txPackets\",\"rxErrs\":\"rxErrs\",
                \"rxPackets\":\"rxPackets\",\"rxBytes\":\"rxBytes\",\"txErrs\":\"txErrs\",\"txBytes\":\"txBytes\",\"rxCo
                mpressed\":\"rxCompressed\"}}"
             3) "ds"
             4) "DS:txDrop:COUNTER:8640:U:U DS:txColls:COUNTER:8640:U:U DS:rxFifo:COUNTER:8640:U:U DS:txCarrier:COUNTER:
                8640:U:U DS:txCompressed:COUNTER:8640:U:U DS:rxFrame:COUNTER:8640:U:U DS:rxDrop:COUNTER:8640:U:U DS:txFi
                fo:COUNTER:8640:U:U DS:rxMulticast:COUNTER:8640:U:U DS:txPackets:COUNTER:8640:U:U DS:rxErrs:COUNTER:8640
                :U:U DS:rxPackets:COUNTER:8640:U:U DS:rxBytes:COUNTER:8640:U:U DS:txErrs:COUNTER:8640:U:U DS:txBytes:COU
                NTER:8640:U:U DS:rxCompressed:COUNTER:8640:U:U"
             5) "rra"
             6) "RRA:AVERAGE:0.5:1:2880 RRA:AVERAGE:0.5:5:2880 RRA:MAX:0.5:1:2880 RRA:MAX:0.5:5:2880 RRA:MIN:0.5:1:2880
                RRA:MIN:0.5:5:2880"
*/
class CiUpdateHandler
{
    protected DbInventory $inventory;
    protected RedisPerfDataApi $redisApi;
    protected LoggerInterface $logger;
    protected string $position;
    protected Closure $funcFetchNext;
    protected UuidInterface $metricStoreUuid;

    /**
     * @param DbInventory $inventory
     * @param RedisPerfDataApi $redisApi
     * @param UuidInterface $metricStoreUuid
     * @param LoggerInterface $logger
     */
    public function __construct(
        DbInventory      $inventory,
        RedisPerfDataApi $redisApi,
        UuidInterface    $metricStoreUuid,
        LoggerInterface  $logger
    ) {
        $this->inventory = $inventory;
        $this->redisApi = $redisApi;
        $this->logger = $logger;
        $this->funcFetchNext = function () {
            $this->processNextScheduledBatch();
        };
        $this->metricStoreUuid = $metricStoreUuid;
    }

    public function run()
    {
        $this->logger->info("CiUpdateHandler is starting");
        $this->fetchLastPosition()->then($this->funcFetchNext);
    }

    protected function fetchLastPosition(): ExtendedPromiseInterface
    {
        $retry = RetryUnless::succeeding(function () {
            return $this->redisApi->fetchLastCiUpdatePosition();
        })->setInterval(1)->slowDownAfter(60, 10);
        $retry->setLogger($this->logger);

        return $retry->run(Loop::get())->then(function ($position) {
            $this->setInitialPosition($position);
        });
    }

    protected function processNextScheduledBatch()
    {
        $this->readNextBatch()->then(function ($stream) {
            $this->processBulk($stream);
        });
    }

    protected function scheduleNextFetch()
    {
        Loop::get()->futureTick($this->funcFetchNext);
    }

    protected function getStreamData($stream): array
    {
        // $stream looks like this:
        // [0] => [ 'rrd:stream', [ 0 => .. ]
        $stream = $stream[0][1];
        if (empty($stream)) {
            $this->logger->warning('Got an unexpected stream construct, please let us know');
            return [];
        }

        return $stream;
    }

    /**
     * @param $stream
     */
    protected function processBulk($stream)
    {
        if (empty($stream)) {
            $this->scheduleNextFetch();
            return;
        }

        $stream = $this->getStreamData($stream);

        try {
            foreach ($stream as $entry) {
                $result = RedisUtil::makeHash($entry[1]);
                $config = JsonString::decode($result->config);
                // $config->uuid, filename, dsNames, dsMap(!!)
                $this->inventory->registerFile(
                    Uuid::fromString($config->uuid),
                    Ci::fromSerialization(JsonString::decode($result->ci)),
                    $this->metricStoreUuid,
                    $config->filename,
                    $step = 60,
                    DsList::fromString($result->ds),
                    RraSet::fromString($result->rra)
                );

                $this->position = $entry[0];
            }
        } catch (\Throwable $e) {
            $this->logger->error($e->getMessage());
        }
        $this->logger->debug('Setting position: ' . $this->position);
        $this->redisApi->setLastCiUpdatePosition($this->position);

        sleep(10)->then($this->funcFetchNext);
    }

    protected function setInitialPosition($position)
    {
        if (empty($position)) {
            $this->logger->info('Got no former position, fetching full CI update stream');
            $this->position = 0;
        } else {
            $this->logger->info("Resuming CI update stream from $position");
            $this->position = $position;
        }
    }

    protected function readNextBatch(): ExtendedPromiseInterface
    {
        $blockMs = '1000';
        return $this->redisApi->fetchBatchFromCiUpdateStream($this->position, 1000, $blockMs);
    }
}
