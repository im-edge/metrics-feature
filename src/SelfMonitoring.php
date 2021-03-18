<?php

namespace IcingaGraphing;

use Evenement\EventEmitterInterface;
use Evenement\EventEmitterTrait;
use gipfl\LinuxHealth\Cpu;
use Psr\Log\LoggerInterface;
use React\EventLoop\LoopInterface;

class SelfMonitoring implements EventEmitterInterface
{
    use EventEmitterTrait;

    protected $loop;

    protected $redisApi;

    protected $logger;

    protected $ciName;

    protected $fetchingRedis;

    public function __construct(
        LoopInterface $loop,
        RedisPerfDataApi $redisApi,
        LoggerInterface $logger,
        $ciName = '000-icinga-rrd-node'
    ) {
        $this->loop = $loop;
        $this->redisApi = $redisApi;
        $this->logger = $logger;
        $this->ciName = $ciName;
        $this->run();
    }

    protected function emitRedisCounters()
    {
        if ($this->fetchingRedis) {
            return;
        }
        $this->fetchingRedis = $this->redisApi->getCounters()->then(function ($counters) {
            $this->emit('perfData', [
                new PerfData($this->ciName . '/RRDHealth', $counters, time())
            ]);
        })->always(function () {
            $this->fetchingRedis = null;
        });
    }

    protected function emitCpuPerformance()
    {
        $counters = Cpu::getCounters();
        $flat = [];
        foreach ($counters as $cpu => $cpuCounters) {
            foreach ($cpuCounters as $label => $counter) {
                $flat["$cpu.$label"] = $counter . 'c';
            }
        }
        $this->emit('perfData', [
            new PerfData($this->ciName . '/RRDHost', $flat, time())
        ]);
    }

    protected function run()
    {
        $this->loop->addPeriodicTimer(1, function () {
            $this->emitRedisCounters();
            $this->emitCpuPerformance();
        });

        $this->logger->info("SelfHealthChecker is ready to start");
    }
}
