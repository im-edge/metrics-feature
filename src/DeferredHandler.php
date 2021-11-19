<?php

namespace IcingaMetrics;

use Exception;
use gipfl\RrdTool\AsyncRrdtool;
use gipfl\RrdTool\RrdCached\Client as RrdCachedClient;
use Psr\Log\LoggerInterface;
use React\EventLoop\LoopInterface;
use function count;
use function ctype_digit;
use function key;

class DeferredHandler
{
    /** @var Store */
    protected $store;

    /** @var LoopInterface */
    protected $loop;

    /** @var RedisPerfDataApi */
    protected $redisApi;

    /** @var LoggerInterface */
    protected $logger;

    protected $pendingCi = [];

    protected $checking;

    protected $rrdCached;

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

    public function run(LoopInterface $loop)
    {
        $this->loop = $loop;
        AsyncDependencies::waitFor('DeferredHandler', [
            'Redis'     => $this->redisApi->getRedisConnection(),
            'RrdCached' => $this->rrdCached->stats(),
        ], 5, $loop, $this->logger)->then(function () {
            $this->loop->addPeriodicTimer(1, function () {
                if (! $this->checkForDeferred()) {
                    $this->logger->warning('Deferred check/handler is still running');
                }
            });
        });
    }

    protected function checkForDeferred()
    {
        if ($this->checking || ! empty($this->pendingCi)) {
            return true;
        }

        $this->checking = $this->redisApi->fetchDeferred()->then(function ($cis) {
            if (empty($cis)) {
                return true;
            }
            foreach ($cis as $ci => $cTime) {
                if (ctype_digit($cTime)) {
                    $this->pendingCi[$ci] = $ci;
                }
                // TODO: (else) handle manual deferred, json_decode, -> reason
            }
            $cntPending = count($this->pendingCi);
            $cntDeferred = count($cis);
            if ($cntDeferred === $cntPending) {
                $this->logger->debug(sprintf('%d deferred CIs ready to process', $cntPending));
            } else {
                $this->logger->debug(sprintf('%d out of %s deferred CIs ready to process', $cntPending, $cntDeferred));
            }
            $this->scheduleDeferredHandler();

            return true;
        }, function (Exception $e) {
            $this->logger->error('DeferredHandler failed to fetchDeferredCids: ' . $e->getMessage());
        });

        return true;
    }

    protected function scheduleDeferredHandler()
    {
        $this->loop->futureTick(function () {
            $ci = key($this->pendingCi);
            if ($ci !== null) {
                $this->handleDeferredCi($ci);
            }
        });
    }

    protected function handleDeferredCi($ci)
    {
        // TODO:
        // error handling
        // if exists -> immediate
        // else -> only if count >= 3
        $this->redisApi->getFirstDeferredPerfDataForCi($ci)
            ->then(function (PerfData $perfData) use ($ci) {
                // $base = 1;
                $base = 60; // 60 second base for now

                return $this->store->wantCi($ci, $perfData, $base);
            })->then(function () use ($ci) {
                $this->logger->debug("DeferredHandler: rescheduling all entries for $ci");
                return $this->redisApi->rescheduleDeferredCi($ci);
            })->then(function () use ($ci) {
                $this->logger->debug("DeferredHandler: done with $ci");
                if (empty($this->pendingCi)) {
                    $this->logger->debug("DeferredHandler: no more CI pending");
                }
            })->otherwise(function (Exception $e) {
                $this->logger->error($e->getMessage());
                throw $e;
            })->always(function () use ($ci) {
                unset($this->pendingCi[$ci]);
                $this->scheduleDeferredHandler();
            });
    }
}
