<?php

namespace IcingaMetrics;

use gipfl\IcingaPerfData\Ci;
use gipfl\Json\JsonString;
use gipfl\RrdTool\AsyncRrdtool;
use gipfl\RrdTool\RrdCached\Client as RrdCachedClient;
use Psr\Log\LoggerInterface;
use React\EventLoop\Loop;
use React\Promise\ExtendedPromiseInterface;
use function count;
use function key;

class DeferredHandler
{
    protected Store $store;
    protected RedisPerfDataApi $redisApi;
    protected RrdCachedClient $rrdCached;
    protected LoggerInterface $logger;
    protected ?ExtendedPromiseInterface $checking = null;
    protected array $pendingCi = [];

    // TODO
    // Infrastructure needs to be always ready,
    // if one of them fails eventually keep fetching stats
    // from the others, but stop working and get into failed
    // state. Or exit and let the parent process deal with this
    public function __construct(
        RedisPerfDataApi $redisApi,
        RrdCachedClient $rrdCached,
        AsyncRrdtool $rrdtool,
        LoggerInterface $logger
    ) {
        $this->redisApi = $redisApi;
        $this->logger = $logger;
        $this->rrdCached = $rrdCached;
        $this->store = new Store($redisApi, $rrdCached, $rrdtool, $logger);
    }

    public function run()
    {
        try {
            AsyncDependencies::waitFor('DeferredHandler', [
                'Redis'     => $this->redisApi->getRedisConnection(),
                'RrdCached' => $this->rrdCached->stats(),
            ], 5, $this->logger)->then(function () {
                Loop::get()->addPeriodicTimer(1, function () {
                    if (! $this->checkForDeferred()) {
                        $this->logger->warning('Deferred check/handler is still processing');
                    }
                });
            });
        } catch (\Throwable $exception) {
            $this->logger->error($exception->getMessage());
        }
    }

    protected function checkForDeferred(): bool
    {
        if ($this->checking) {
            $this->logger->notice('Still waiting for deferred items from Redis');
            return false;
        }
        if (! empty($this->pendingCi)) {
            $this->logger->debug(sprintf('There are still %d items pending:', count($this->pendingCi)));
            return false;
        }

        $this->checking = $this->redisApi->fetchDeferred()->then(function ($cis) {
            $this->checking = null;
            if (empty($cis)) {
                return true;
            }
            try {
                $cntDeferred = 0;
                foreach ($cis as $ci => $ciDetails) {
                    $ciDetails = JsonString::decode($ciDetails);
                    if ($ciDetails->reason === 'Unknown CI'
                        || preg_match('/^Unknown DS name /', $ciDetails->reason)  // Unknown DS name "value2"
                    ) {
                        $this->pendingCi[$ci] = new PerfData(
                            Ci::fromSerialization(JsonString::decode($ci)),
                            (array) $ciDetails->dataPoints,
                            (int) $ciDetails->ts
                        );
                        $cntDeferred++;
                    }
                    // TODO: (else) handle manual deferred, json_decode, -> reason
                }
                $cntPending = count($this->pendingCi);
                if ($cntPending > 0) {
                    if ($cntDeferred === $cntPending) {
                        $this->logger->debug(sprintf('%d deferred CIs ready to process', $cntPending));
                    } else {
                        $this->logger->debug(sprintf(
                            '%d out of %s deferred CIs ready to process',
                            $cntPending,
                            $cntDeferred
                        ));
                    }
                    $this->scheduleDeferredHandler();
                }
            } catch (\Throwable $e) {
                // TODO: And now??
                $this->logger->error($e->getMessage());
            }

            return true;
        }, function (\Throwable $e) {
            $this->checking = null;
            $this->logger->error('DeferredHandler failed to fetchDeferredCids: ' . $e->getMessage());
        });

        return true;
    }

    protected function scheduleDeferredHandler()
    {
        // This is useless, we can skip it. WHY?
        Loop::get()->futureTick(function () {
            $ci = key($this->pendingCi);
            if ($ci !== null) {
                $this->handleDeferredCi($ci, $this->pendingCi[$ci]);
            }
        });
    }

    protected function handleDeferredCi($ci, PerfData $perfData)
    {
        $base = 60; // 60 seconds base for now
        return $this->store->wantCi($ci, $perfData, $base)->then(function () use ($ci) {
            $this->logger->debug("DeferredHandler: rescheduling all entries for $ci");
            return $this->redisApi->rescheduleDeferredCi($ci);
        })->then(function () use ($ci) {
            if (empty($this->pendingCi)) {
                $this->logger->debug("DeferredHandler: done with $ci, no more CI pending");
            } else {
                $this->logger->debug("DeferredHandler: done with $ci");
            }
            unset($this->pendingCi[$ci]);
            $this->scheduleDeferredHandler();
        }, function ($e) use ($ci) {
            $this->logger->error($e->getMessage());
            unset($this->pendingCi[$ci]);
            $this->scheduleDeferredHandler();
        });
    }
}
