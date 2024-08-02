<?php

namespace IMEdge\MetricsFeature\Rrd;

use Amp\ByteStream\WritableResourceStream;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\Process\Process;
use Amp\TimeoutCancellation;
use Exception;
use IMEdge\Metrics\Metric;
use IMEdge\Metrics\MetricDatatype;
use IMEdge\ProcessRunner\BufferedLineReader;
use IMEdge\ProcessRunner\ProcessRunnerHelper;
use RuntimeException;
use SplDoublyLinkedList;

use function Amp\async;
use function Amp\ByteStream\pipe;

class RrdtoolRunner extends ProcessRunnerHelper
{
    protected const IMAGE_BLOB_PREFIX = 'image = BLOB_SIZE:';
    protected const IMAGE_BLOB_PREFIX_LENGTH = 18;
    protected const EOL = "\n";

    protected string $applicationName = 'rrdtool';
    protected string $locale = 'en_US.utf8';
    protected string $timezone = 'Europe/Berlin';
    protected bool $logCommunication = false;
    protected string $buffer = '';
    protected string $currentBuffer = '';
    protected ?int $pendingImageBytes = null;
    protected ?string $rrdCachedSocket = null;
    /** @var SplDoublyLinkedList<DeferredFuture> */
    protected SplDoublyLinkedList $pending;
    protected SplDoublyLinkedList $pendingStartTimes;

    protected ?TimeStatistics $spentTimings = null;
    protected int $imageCount = 0;
    protected int $imageSize = 0;
    protected int $requestCount = 0;
    protected int $octetsIn = 0;
    protected int $octetsOut = 0;
    protected ?string $processStatsLine = null;
    protected WritableResourceStream $processStdin;

    protected function initialize(): void
    {
        $this->pending = new SplDoublyLinkedList();
        $this->pendingStartTimes = new SplDoublyLinkedList();
        $this->spentTimings = new TimeStatistics(0, 0, 0);
    }

    public function send(string $command)
    {
        $this->octetsIn += strlen($command);
        $this->requestCount++;
        $this->pending->push($deferred = new DeferredFuture());
        $this->pendingStartTimes->push(hrtime(true));

        if ($this->logCommunication) {
            $this->logger->debug("> $command");
        }
        $this->processStdin->write("$command\n");

        // TODO: better timeout/cancellation handling
        return $deferred->getFuture()->await(new TimeoutCancellation(10));
    }

    public function sendAsync(string $command): Future
    {
        $this->pending->push($deferred = new DeferredFuture());
        $this->pendingStartTimes->push(hrtime(true));

        if ($this->logCommunication) {
            $this->logger->debug("> $command");
        }
        $this->processStdin->write("$command\n");

        // TODO: better timeout/cancellation handling
        return $deferred->getFuture();
    }

    /**
     * @return Metric[]
     */
    public function getMetrics(): array
    {
        $timings = $this->spentTimings;
        return [
            new Metric('timeSpentSystem', $timings->system, MetricDatatype::DDERIVE),
            new Metric('timeSpentUser', $timings->user, MetricDatatype::DDERIVE),
            new Metric('timeSpentReal', $timings->real, MetricDatatype::DDERIVE),
            new Metric('imageCount', $this->imageCount, MetricDatatype::COUNTER),
            new Metric('requestCount', $this->requestCount, MetricDatatype::COUNTER),
            new Metric('imageSize', $this->imageSize, MetricDatatype::COUNTER),
            new Metric('octetsIn', $this->octetsIn, MetricDatatype::COUNTER),
            new Metric('octetsOut', $this->octetsOut, MetricDatatype::COUNTER),
        ];
    }

    public function setRrdCachedSocket(string $socketDir): void
    {
        $this->rrdCachedSocket = $socketDir;
    }

    public function setLocale(string $locale): void
    {
        $this->locale = $locale;
    }

    public function setTimezone(string $timezone): void
    {
        $this->timezone = $timezone;
    }

    public function getGlobalSpentTimings(): TimeStatistics
    {
        return $this->spentTimings;
    }

    protected function processData(string $data): void
    {
        if ($this->logCommunication) {
            $this->logger->debug("< $data");
        }
        $this->octetsOut += strlen($data);
        $this->buffer .= $data;
        $this->processBuffer();
    }

    protected function consumeBinaryBuffer(): void
    {
        $bufferLength = strlen($this->buffer);
        if ($bufferLength < $this->pendingImageBytes) {
            $this->currentBuffer .= $this->buffer;
            $this->pendingImageBytes -= $bufferLength;
            $this->buffer = '';
            // $this->logger->info(sprintf('Got %dbytes, missing %d', $bufferLength, $this->pendingImageBytes));
            return;
        } else {
            // $this->logger->info(sprintf('Buffer has full image, adding missing %dbytes', $this->pendingImageBytes));
            $this->currentBuffer .= substr($this->buffer, 0, $this->pendingImageBytes);
            $this->buffer = substr($this->buffer, $this->pendingImageBytes);
            $this->imageSize += $this->pendingImageBytes;
            $this->imageCount++;
            /*
            $this->logger->info(sprintf(
                'Got all, binary is done. Buffer has %d bytes: %s',
                strlen($this->buffer),
                var_export(substr($this->buffer, 0, 60), 1)
            ));
            */
            $this->pendingImageBytes = null;
        }
    }

    protected function processBuffer(): void
    {
        if ($this->pendingImageBytes) {
            $this->consumeBinaryBuffer();
        }
        if ($this->pendingImageBytes) {
            return;
        }

        $offset = 0;
        while (false !== ($pos = \strpos($this->buffer, self::EOL, $offset))) {
            $line = substr($this->buffer, $offset, $pos - $offset);
            $offset = $pos + 1;

            // Let's handle valid results
            if (str_starts_with($line, 'OK ')) {
                // OK u:1.14 s:0.07 r:1.21
                // Might be 1,14 with different locale
                $this->processStatsLine = substr($line, 3);
                $this->spentTimings = TimeStatistics::parse($line);
                // TODO: add "\n" ?
                // $this->logger->info(sprintf('Got OK, resolving with %dbytes', strlen($this->currentBuffer)));
                $this->resolveNextPending($this->currentBuffer);
                $this->currentBuffer = '';
            } elseif (str_starts_with($line, 'ERROR: ')) {
                $this->rejectNextPending($line);
                $this->currentBuffer = '';
            } elseif (substr($line, 0, self::IMAGE_BLOB_PREFIX_LENGTH) === self::IMAGE_BLOB_PREFIX) {
                $this->pendingImageBytes = (int) substr($line, self::IMAGE_BLOB_PREFIX_LENGTH);
                // TODO: don't log, but collect/emit metrics
                // $this->logger->debug(sprintf('Waiting for an image, %dbytes: %s', $this->pendingImageBytes, $line));
                $this->currentBuffer .= $line . self::EOL;
                $this->buffer = substr($this->buffer, $offset);
                $this->consumeBinaryBuffer();
                $this->processBuffer();
                return;
            } else {
                $this->currentBuffer .= $line . self::EOL;
            }
        }

        if ($offset !== 0) {
            $this->buffer = substr($this->buffer, $offset);
        }
    }

    protected function failForProtocolViolation(): void
    {
        $exception = new RuntimeException('Protocol exception, got: ' . $this->getFullBuffer());
        $this->rejectAllPending($exception);
        $this->stop();
    }

    protected function resolveNextPending($result): void
    {
        $deferred = $this->pending->shift();
        assert($deferred instanceof DeferredFuture);
        $duration = hrtime(true) - $this->pendingStartTimes->shift();
        $deferred->complete($result);
    }

    protected function rejectNextPending($message): void
    {
        $deferred = $this->pending->shift();
        assert($deferred instanceof DeferredFuture);
        $duration = hrtime(true) - $this->pendingStartTimes->shift();
        $deferred->error(new RuntimeException($message));
    }

    protected function rejectAllPending(Exception $exception): void
    {
        while (! $this->pending->isEmpty()) {
            $this->pending->shift()->reject($exception);
        }
    }

    protected function getFullBuffer(): string
    {
        if (empty($this->currentBuffer)) {
            return $this->buffer;
        }

        return $this->currentBuffer . "\n" . $this->buffer;
    }

    protected function onProcessStarted(Process $process): void
    {
        async(function () use ($process) {
            $this->processStdin = $process->getStdin();
            while (null !== ($data = $process->getStdout()->read())) {
                $this->processData($data);
            }
            $stdErrReader = new BufferedLineReader($this->logger->error(...), "\n");
            pipe($process->getStderr(), $stdErrReader);
        });
    }

    protected function getArguments(): array
    {
        return ['-'];
    }

    public function getEnv(): array
    {
        $env = [
            'TZ'     => $this->timezone,
            'LC_ALL' => $this->locale,
        ];
        if ($this->rrdCachedSocket !== null) {
            $env['RRDCACHED_ADDRESS'] = $this->rrdCachedSocket;
        }

        return $env + ['AMP_DEBUG' => 'true'];
    }
}
