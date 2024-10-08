<?php

namespace IMEdge\MetricsFeature;

use DirectoryIterator;
use Evenement\EventEmitterInterface;
use Evenement\EventEmitterTrait;
use IMEdge\IcingaPerfData\PerfDataFile;
use IMEdge\Metrics\Ci;
use IMEdge\Metrics\Measurement;
use IMEdge\Metrics\Metric;
use IMEdge\Metrics\MetricDatatype;
use IMEdge\Metrics\MetricsEvent;
use Psr\Log\LoggerInterface;
use Ramsey\Uuid\UuidInterface;
use Revolt\EventLoop;

use function count;
use function microtime;
use function preg_match;
use function sort;

class PerfDataShipper implements EventEmitterInterface
{
    use EventEmitterTrait;

    protected const CNT_PARSED_LINES = 'parsed_lines';
    protected const CNT_FAILED_LINES = 'failed_lines';
    protected const CNT_PROCESSED_BYTES = 'processed_bytes';
    protected const CNT_PROCESSED_METRICS = 'processed_metrics';

    protected const TIME_DIRECTORY_SCAN_NS = 'time_directory_scan_ns';
    protected const TIME_FILE_READ_NS = 'time_file_read_ns';
    protected const TIME_FILE_PARSE_NS = 'time_file_parse_ns';
    protected const TIME_EMIT_MEASUREMENT_NS = 'time_emit_measurement_ns';

    protected const COUNTERS = [
        self::CNT_PARSED_LINES,
        self::CNT_FAILED_LINES,
        self::CNT_PROCESSED_BYTES,
        self::CNT_PROCESSED_METRICS,
    ];

    protected const TIMES_NS = [
        self::TIME_DIRECTORY_SCAN_NS,
        self::TIME_FILE_READ_NS,
        self::TIME_FILE_PARSE_NS,
        self::TIME_EMIT_MEASUREMENT_NS
    ];

    protected array $counters = [];
    protected array $times = [];

    protected const DELAY_WHEN_IDLE = 5;

    protected LoggerInterface $logger;
    protected UuidInterface $storeUuid;
    protected Ci $ci;
    protected array $files = [];
    protected string $dir;
    protected bool $paused = false;
    protected ?string $statsTimer = null;
    protected ?int $currentIdx = null;

    public function __construct(LoggerInterface $logger, UuidInterface $storeUUid, string $dir)
    {
        $this->storeUuid = $storeUUid;
        $this->logger = $logger;
        $this->dir = $dir;
        $this->ci = new Ci($this->storeUuid->toString(), 'PerfDataShipper');

        $this->initializeCounters();
    }

    public function run(): void
    {
        EventLoop::queue($this->emitCounters(...));
        $this->statsTimer = EventLoop::repeat(15, $this->emitCounters(...));
        EventLoop::queue($this->scanFiles(...));
        $this->logger->notice(sprintf('PerfDataShipper for %s is ready', $this->dir));
    }

    public function stop(): void
    {
        if ($this->statsTimer) {
            EventLoop::cancel($this->statsTimer);
        }
    }

    public function pause(): void
    {
        $this->paused = true;
    }

    public function resume(): void
    {
        $this->paused = false;
    }

    protected function emitCounters(): void
    {
        $this->emit(MetricsEvent::ON_MEASUREMENTS, [[new Measurement($this->ci, time(), $this->getMetrics())]]);
    }

    protected function getMetrics(): array
    {
        $metrics = [];
        foreach ($this->counters as $counter => $value) {
            $metrics[] = new Metric($counter, $value, MetricDatatype::COUNTER);
        }
        foreach ($this->times as $label => $value) {
            $metrics[] = new Metric($label, $value, MetricDatatype::GAUGE, 'ns');
        }

        return $metrics;
    }

    protected function initializeCounters(): void
    {
        foreach (self::COUNTERS as $counter) {
            $this->counters[$counter] = 0;
        }
        foreach (self::TIMES_NS as $metric) {
            $this->times[$metric] = 0;
        }
    }

    protected function scanFiles(): void
    {
        if ($this->paused) {
            return;
        }
        if (! empty($this->files)) {
            throw new \RuntimeException('Cannot scan files, queue is not empty');
        }
        $start = microtime(true);
        $directory = new DirectoryIterator($this->dir);
        foreach ($directory as $file) {
            if ($file->isFile() && $file->isReadable()) {
                $filename = $file->getBasename();
                if (preg_match('/\.\d+$/', $filename)) {
                    $this->files[] = $filename;
                }
            }
        }
        sort($this->files);
        $this->stopTime(self::TIME_DIRECTORY_SCAN_NS, $start);
        $this->processFiles();
    }

    protected function scheduleNextFile(): void
    {
        if ($this->paused) {
            EventLoop::delay(5, $this->scheduleNextFile(...));
            return;
        }
        EventLoop::queue($this->processNextFile(...));
    }

    protected function stopTime(string $name, float $start): void
    {
        $this->times[$name] += floor(1000000 * (microtime(true) - $start));
    }

    public function processNextFile(): void
    {
        $filename = $this->files[$this->currentIdx];

        $start = microtime(true);
        $content = \file($this->dir . "/$filename");
        $this->stopTime(self::TIME_FILE_READ_NS, $start);

        $allMeasurements = [];
        foreach ($content as $line) {
            try {
                $start = microtime(true);
                $measurements = PerfDataFile::parseLine($line);
                $this->counters[self::CNT_PARSED_LINES]++;
                $this->stopTime(self::TIME_FILE_PARSE_NS, $start);
            } catch (\Exception $e) {
                $this->logger->error('Metrics error: ' . $e->getMessage());
                $measurements = [];
                $this->counters[self::CNT_FAILED_LINES]++;
            }

            $this->counters[self::CNT_PROCESSED_BYTES] += strlen($line);
            if (! empty($measurements)) {
                $cntMetrics = 0;
                foreach ($measurements as $measurement) {
                    $cntMetrics += $measurement->countMetrics();
                }
                $this->counters[self::CNT_PROCESSED_METRICS] += $cntMetrics;
                foreach ($measurements as $measurement) {
                    $allMeasurements[] = $measurement;
                }
            }
        }
        if (!empty($allMeasurements)) {
            $this->emit(MetricsEvent::ON_MEASUREMENTS, [$allMeasurements]);
        }
        $this->logger->debug("Processed $filename");
        unlink($this->dir . "/$filename");
        $this->tickNext();
    }

    protected function tickNext(): void
    {
        $this->currentIdx++;
        if (isset($this->files[$this->currentIdx])) {
            $this->scheduleNextFile();
        } else {
            $this->files = [];
            $this->currentIdx = null;
            EventLoop::queue($this->scanFiles(...));
        }
    }

    protected function processFiles(): void
    {
        if (count($this->files)) {
            $this->currentIdx = 0;
            $this->scheduleNextFile();
        } else {
            EventLoop::delay(self::DELAY_WHEN_IDLE, $this->scanFiles(...));
            $this->currentIdx = null;
        }
    }
}
