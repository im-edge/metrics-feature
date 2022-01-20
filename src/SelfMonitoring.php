<?php

namespace IcingaMetrics;

use Evenement\EventEmitterInterface;
use Evenement\EventEmitterTrait;
use gipfl\IcingaPerfData\Ci;
use gipfl\LinuxHealth\Cpu;
use gipfl\LinuxHealth\Network;
use gipfl\RrdTool\RrdCached\Client;
use Psr\Log\LoggerInterface;
use React\EventLoop\Loop;
use React\Promise\ExtendedPromiseInterface;

class SelfMonitoring implements EventEmitterInterface
{
    use EventEmitterTrait;

    protected RedisPerfDataApi $redisApi;
    protected Client $rrdCached;
    protected LoggerInterface $logger;
    protected ?ExtendedPromiseInterface $fetchingRedis = null;
    protected string $ciName;

    public function __construct(
        RedisPerfDataApi $redisApi,
        Client $rrdCached,
        LoggerInterface $logger,
        $ciName
    ) {
        $this->redisApi = $redisApi;
        $this->rrdCached = $rrdCached;
        $this->logger = $logger;
        $this->ciName = $ciName;
    }

    protected function emitRedisCounters()
    {
        if ($this->fetchingRedis) {
            $this->logger->notice('Redis counters are overdue');
            return;
        }
        $this->fetchingRedis = $this->redisApi->getCounters()->then(function ($counters = null) {
            $this->fetchingRedis = null;
            $this->emit('perfData', [
                new PerfData(new Ci($this->ciName, 'RRDHealth'), $this->makeCounters((array) $counters), time())
            ]);
        }, function (\Throwable $e) {
            $this->fetchingRedis = null;
            $this->logger->error($e->getMessage());
        });
    }

    protected function emitRrdCachedCounters()
    {
        $this->rrdCached
            ->stats()
            ->then(function ($result) {
                foreach ($result as $key => & $value) {
                    if (! in_array($key, ['QueueLength', 'TreeNodesNumber', 'TreeDepth'])) {
                        $value .= 'c';
                    }
                }
                unset($value);
                $this->emit('perfData', [
                    new PerfData(new Ci($this->ciName, 'RRDCacheD'), $result, time())
                ]);
            }, function (\Exception $e) {
                $this->logger->error('SelfHealthCheck got no data from RRDCacheD: ' . $e->getMessage());
            });
    }

    protected function emitInterfaceCounters()
    {
        foreach (Network::getInterfaceCounters() as $ifName => $counters) {
            $this->emit('perfData', [
                new PerfData(
                    new Ci($this->ciName, 'Interface', $ifName),
                    $this->makeCounters((array) $counters),
                    time()
                )
            ]);
        }
    }

    protected function makeCounters(array $counters): array
    {
        $result = [];
        foreach ($counters as $key => $value) {
            $result[$key] = $value . 'c';
        }

        return $result;
    }

    protected function emitCpuPerformance()
    {
        $counters = Cpu::getCounters();
        foreach ($counters as $cpu => $cpuCounters) {
            $this->emit('perfData', [
                new PerfData(new Ci($this->ciName, 'CPU', $cpu), $this->makeCounters($cpuCounters), time())
            ]);
        }
    }

    public function run($interval)
    {
        Loop::get()->addPeriodicTimer($interval, function () {
            $this->emitRedisCounters();
            $this->emitRrdCachedCounters();
            $this->emitCpuPerformance();
            $this->emitInterfaceCounters();
        });

        $this->logger->info("SelfHealthChecker is ready to start");
    }
}
