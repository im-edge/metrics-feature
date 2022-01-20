<?php

namespace IcingaMetrics;

use DirectoryIterator;
use Evenement\EventEmitterInterface;
use Evenement\EventEmitterTrait;
use gipfl\IcingaPerfData\Ci;
use gipfl\IcingaPerfData\Measurement;
use gipfl\IcingaPerfData\Parser\PerfDataFile;
use Psr\Log\LoggerInterface;
use React\EventLoop\Loop;
use React\EventLoop\TimerInterface;
use function count;
use function microtime;
use function preg_match;
use function sort;

class PerfDataShipper implements EventEmitterInterface
{
    use EventEmitterTrait;

    public const ON_MEASUREMENT = 'measurement';
    public const ON_MEASUREMENTS = 'measurements';

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

    protected const DELAY_WHEN_IDLE = 5;

    protected LoggerInterface $logger;
    protected Measurement $total;
    protected ?TimerInterface $statsTimer = null;

    protected string $dir;
    protected array $files = [];
    protected ?int $currentIdx = null;

    protected bool $paused = false;

    public function __construct(LoggerInterface $logger, string $dir)
    {
        $this->logger = $logger;
        $this->dir = $dir;

        Loop::addTimer(1, function () {
            $this->emit(self::ON_MEASUREMENT, [$this->total]);
        });
    }

    public function run()
    {
        $this->initializeCounters();
        $this->emitCounters();
        $this->statsTimer = Loop::addPeriodicTimer(15, function () {
            $this->emitCounters();
        });
        $this->scanFiles();
    }

    public function stop()
    {
        if ($this->statsTimer) {
            Loop::cancelTimer($this->statsTimer);
        }
    }

    public function pause()
    {
        $this->paused = true;
    }

    public function resume()
    {
        $this->paused = false;
    }

    protected function emitCounters()
    {
        $this->emit(self::ON_MEASUREMENT, [$this->total]);
    }

    protected function initializeCounters()
    {
        $total = new Measurement(new Ci('PerfDataShipper', 'PerfDataShipper'));
        foreach (self::COUNTERS as $counter) {
            $total->incrementCounter($counter, 0);
        }
        foreach (self::TIMES_NS as $metric) {
            $total->setGaugeValue($metric, 0);
            $total->getMetric($metric)->setUnit('ns');
        }
        $this->total = $total;
    }

    protected function scanFiles()
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

    protected function scheduleNextFile()
    {
        if ($this->paused) {
            Loop::addTimer(5, function () {
                $this->scheduleNextFile();
            });
            return;
        }
        Loop::futureTick(function () {
            $this->processNextFile();
        });
    }

    protected function stopTime(string $name, float $start)
    {
        $this->total->incrementCounter($name, floor(1000000 * (microtime(true) - $start)));
    }

    public function processNextFile()
    {
        $filename = $this->files[$this->currentIdx];

        $start = microtime(true);
        $content = \file($this->dir . "/$filename");
        $this->stopTime(self::TIME_FILE_READ_NS, $start);

        $allMeasurements = [];
        foreach ($content as $line) {
            $total = $this->total;
            try {
                $start = microtime(true);
                $measurements = PerfDataFile::parseLine($line);
                $total->incrementCounter(self::CNT_PARSED_LINES);
                $this->stopTime(self::TIME_FILE_PARSE_NS, $start);
            } catch (\Exception $e) {
                echo $e->getMessage() . "\n";
                $measurements = [];
                $total->incrementCounter(self::CNT_FAILED_LINES);
            }

            $total->incrementCounter(self::CNT_PROCESSED_BYTES, strlen($line));
            if (! empty($measurements)) {
                $cntMetrics = 0;
                foreach ($measurements as $measurement) {
                    $cntMetrics += $measurement->countMetrics();
                }
                $total->incrementCounter(self::CNT_PROCESSED_METRICS, $cntMetrics);
                foreach ($measurements as $measurement) {
                    $allMeasurements[] = $measurement;
                    // Single measurement - replaced by bulk. We might also want to buffer chunks
                    // $this->emit(self::ON_MEASUREMENT, [$measurement]);
                }
            }
        }
        if (!empty($allMeasurements)) {
            $this->emit(self::ON_MEASUREMENTS, [$allMeasurements]);
        }
        $this->logger->debug("Processed $filename");
        unlink($this->dir . "/$filename");
        $this->tickNext();
    }

    protected function tickNext()
    {
        $this->currentIdx++;
        if (isset($this->files[$this->currentIdx])) {
            $this->scheduleNextFile();
        } else {
            $this->files = [];
            $this->currentIdx = null;
            Loop::futureTick(function () {
                $this->scanFiles();
            });
        }
    }

    protected function processFiles()
    {
        if (count($this->files)) {
            $this->currentIdx = 0;
            $this->scheduleNextFile();
        } else {
            Loop::addTimer(self::DELAY_WHEN_IDLE, function () {
                $this->scanFiles();
            });
            $this->currentIdx = null;
        }
    }
}
