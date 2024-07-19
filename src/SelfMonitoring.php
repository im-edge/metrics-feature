<?php

namespace IMEdge\MetricsFeature;

use Amp\Redis\RedisClient;
use Evenement\EventEmitterInterface;
use Evenement\EventEmitterTrait;
use gipfl\LinuxHealth\Cpu;
use gipfl\LinuxHealth\Memory;
use gipfl\LinuxHealth\Network;
use IMEdge\Metrics\Ci;
use IMEdge\Metrics\Measurement;
use IMEdge\Metrics\Metric;
use IMEdge\Metrics\MetricDatatype;
use IMEdge\MetricsFeature\Rrd\RrdtoolRunner;
use IMEdge\ProcessRunner\ProcessWithPidInterface;
use IMEdge\RedisUtils\RedisResult;
use IMEdge\RrdCached\RrdCachedClient;
use Psr\Log\LoggerInterface;
use Revolt\EventLoop;

class SelfMonitoring implements EventEmitterInterface
{
    use EventEmitterTrait;

    protected string $prefix = 'metrics:'; // TODO: const, elsewhere
    protected ?string $timer = null;
    /** @var ProcessWithPidInterface[] */
    protected array $processRunners;

    public function __construct(
        protected readonly RedisClient $redis,
        protected readonly RrdCachedClient $rrdCached,
        protected readonly RrdtoolRunner $rrdtoolRunner,
        protected readonly LoggerInterface $logger,
        protected readonly string $ciHostName
    ) {
    }

    public function watchProcessRunners(array $runners): void
    {
        foreach ($runners as $name => $runner) {
            $this->setProcessRunner($name, $runner);
        }
    }

    public function setProcessRunner($name, ProcessWithPidInterface $runner): void
    {
        $this->processRunners[$name] = $runner;
    }

    public function run(int $interval): void
    {
        $this->timer = EventLoop::repeat($interval, $this->tick(...));
        $this->logger->info("Metrics SelfHealthChecker is ready to start");
    }

    protected function tick(): void
    {
        $measurements = array_merge(
            $this->getRedisCounters(),
            $this->getRrdCachedCounters(),
            $this->getRrdtoolCounters(),
            $this->getCpuPerformance(),
            $this->getInterfaceCounters(),
            $this->getRedisProcessCounters(),
        );

        if (!empty($measurements)) {
            $this->emit(MetricStoreRunner::ON_MEASUREMENTS, [$measurements]);
        }
    }

    public function stop(): void
    {
        if ($this->timer) {
            EventLoop::cancel($this->timer);
            $this->timer = null;
        }
    }

    protected function getRedisCounters(): array
    {
        $counters = RedisResult::toArray($this->redis->execute('HGETALL', $this->prefix . 'counters'));
        if (empty($counters)) {
            return [];
        }

        return [
            new Measurement(new Ci($this->ciHostName, 'RRDHealth'), time(), self::makeCounterMetrics($counters))
        ];
    }

    protected function getRrdtoolCounters(): array
    {
        return [
            new Measurement(new Ci($this->ciHostName, 'RRDTool'), time(), $this->rrdtoolRunner->getMetrics())
        ];
    }

    protected function getRrdCachedCounters(): array
    {
        try {
            $stats = $this->rrdCached->stats();
        } catch (\Exception $e) {
            $this->logger->error('SelfHealthCheck got no data from RRDCacheD: ' . $e->getMessage());
            return [];
        }

        // TODO: implement getMetrics in Stats class?
        $metrics = [
            new Metric('QueueLength', $stats->queueLength),
            new Metric('TreeNodesNumber', $stats->treeNodesNumber),
            new Metric('TreeDepth', $stats->treeDepth),
            new Metric('UpdatesReceived', $stats->updatesReceived, MetricDatatype::COUNTER),
            new Metric('FlushesReceived', $stats->flushesReceived, MetricDatatype::COUNTER),
            new Metric('UpdatesWritten', $stats->updatesWritten, MetricDatatype::COUNTER),
            new Metric('DataSetsWritten', $stats->dataSetsWritten, MetricDatatype::COUNTER),
            new Metric('JournalBytes', $stats->journalBytes, MetricDatatype::COUNTER),
            new Metric('JournalRotate', $stats->journalRotate, MetricDatatype::COUNTER),
        ];

        return [
            new Measurement(
                new Ci($this->ciHostName, 'RRDCacheD'),
                time(),
                $metrics
            )
        ];
    }

    protected function getInterfaceCounters(): array
    {
        $measurements = [];
        foreach (Network::getInterfaceCounters() as $ifName => $counters) {
            $measurements[] = new Measurement(
                new Ci($this->ciHostName, 'Interface', $ifName),
                time(),
                self::makeCounterMetrics((array) $counters),
            );
        }
        return $measurements;
    }

    protected function getCpuPerformance(): array
    {
        $counters = Cpu::getCounters();
        $measurements = [];
        foreach ($counters as $cpu => $cpuCounters) {
            $measurements[] = new Measurement(
                new Ci($this->ciHostName, 'CPU', $cpu),
                time(),
                self::makeCounterMetrics($cpuCounters)
            );
        }
        return $measurements;
    }

    protected function getRedisProcessCounters(): array
    {
        $measurements = [];
        foreach ($this->processRunners as $name => $runner) {
            if ($pid = $runner->getProcessPid()) {
                $memory = Memory::getUsageForPid($pid);
                if ($memory === false) {
                    continue;
                }
            } else {
                $memory = (object) [
                    'size'   => null,
                    'rss'    => null,
                    'shared' => null,
                ];
            }
            $measurements[] = new Measurement(
                new Ci($this->ciHostName, 'Memory', $name),
                time(),
                [
                    new Metric('size', $memory->size),
                    new Metric('rss', $memory->rss),
                    new Metric('shared', $memory->shared),
                ]
            );
        }

        return $measurements;
    }

    protected static function makeCounterMetrics(array $counters): array
    {
        $result = [];
        foreach ($counters as $key => $value) {
            $result[] = new Metric($key, $value, MetricDatatype::COUNTER);
        }

        return $result;
    }

    public function __destruct()
    {
        if ($this->timer) {
            EventLoop::cancel($this->timer);
            $this->timer = null;
        }
    }
}
