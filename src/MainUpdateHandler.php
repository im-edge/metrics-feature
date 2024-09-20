<?php

namespace IMEdge\MetricsFeature;

use Amp\Redis\RedisClient;
use Exception;
use IMEdge\Json\JsonString;
use IMEdge\Metrics\Format;
use IMEdge\RrdCached\RrdCachedClient;
use IMEdge\RrdCached\RrdCachedCommand;
use Psr\Log\LoggerInterface;
use Revolt\EventLoop;
use RuntimeException;

class MainUpdateHandler
{
    protected string $position;
    protected string $prefix = 'metrics:'; // duplicate, see SelfMonitoring
    protected float $startupTime;
    protected bool $startingUp = true;
    protected bool $shuttingDown = false;
    protected int $backlogLinesAtStartup = 0;
    protected int $backlogBytesAtStartup = 0;
    protected ?string $connecting = null;

    public function __construct(
        protected readonly RedisClient $redis,
        protected readonly RrdCachedClient $rrdCached,
        protected readonly LoggerInterface $logger
    ) {
    }

    public function fetchLastPosition(): ?string
    {
        return $this->redis->execute('GET', $this->prefix . 'stream-last-pos');
    }

    public function setLastPosition(string $position): void
    {
        $this->redis->execute('SET', $this->prefix . 'stream-last-pos', $position);
    }

    public function run(): void
    {
        $this->startupTime = microtime(true);
        $this->logger->notice("MainUpdateHandler is starting");
        $this->setInitialPosition($this->fetchLastPosition());
        $this->processNextScheduledBatch();
    }

    public function stop(): void
    {
        $this->shuttingDown = true;
    }

    protected function processNextScheduledBatch(): void
    {
        if ($this->shuttingDown) {
            return;
        }

        try {
            $this->processBulk($this->readNextBatch());
            EventLoop::queue($this->processNextScheduledBatch(...));
        } catch (Exception $e) {
            if ($this->shuttingDown) {
                return;
            }
            $this->logger->error('Reading next batch failed, continuing in 15s: ' . $e->getMessage());
            EventLoop::delay(15, $this->processNextScheduledBatch(...));
        }
    }

    protected function processBulk(?array $stream): void
    {
        if (empty($stream)) {
            if ($this->startingUp) {
                $this->logStartupInfo();
                $this->startingUp = false;
            }

            return;
        }

        $bulk = [];

        // $stream looks like this:
        // [0] => [ 'rrd:stream', [ 0 => .. ]
        $stream = $stream[0][1] ?? null;
        if (empty($stream)) {
            if ($this->startingUp) {
                $this->startingUp = false;
            }
            $this->logger->warning('Got an unexpected stream construct, please let us know');
            return;
        }

        foreach ($stream as $entry) {
            $this->position = $entry[0];
            // entry = [ 1537893299864-0, [key, val, .., ..]
            if ($entry[1][0] !== 'update') {
                throw new RuntimeException("Got invalid update from stream: " . JsonString::encode($entry, 1));
            }
            $line = $entry[1][1];
            if ($this->startingUp) {
                $this->backlogLinesAtStartup++;
                $this->backlogBytesAtStartup += strlen($line) + 1;
            }
            // $this->logger->notice(RrdCachedCommand::UPDATE . " $line");
            $bulk[] = RrdCachedCommand::UPDATE . " $line";
        }
        if (empty($bulk)) {
            $this->logger->notice('Nothing to send with this batch, pretty strange');
            return;
        }

        try {
            $result = $this->rrdCached->batch($bulk);
            if ($result !== true) {
                $this->processErrors($result, $stream);
            }
            $this->setLastPosition($this->position);
        } catch (Exception $e) {
            $this->logger->error($e->getMessage());
        }
    }

    /**
     * Special treatment for step errors, as there can potentially be LOTS of
     * them
     *
     * @param $result
     * @param $stream
     */
    protected function processErrors($result, $stream): void
    {
        $stepError = 0;
        // TODO: $stepErrorFiles = [];
        foreach ($result as $pos => $error) {
            // rrdCached starts counting with 1
            $realPos = $pos - 1;
            $failedLine = $stream[$realPos][1][1] ?? '<unknown line>';

            if (str_contains($error, 'No such file')) {
                $filename = substr($failedLine, 0, strpos($failedLine, ' '));
                $this->logger->debug(sprintf(
                    'RRDCacheD rejected for missing file "%s" (%s): %s',
                    $filename,
                    $failedLine,
                    $error
                ));
                // TODO: $this->redisApi->deferCi()
            } elseif (str_contains($error, 'minimum one second step')) {
                $stepError++;
            } else {
                $this->logger->debug(sprintf(
                    'RRDCacheD rejected "%s": %s',
                    $failedLine,
                    $error
                ));
            }
        }
        if ($stepError > 0) {
            $this->logger->debug(sprintf(
                'RRDCacheD rejected %d outdated updates',
                $stepError
            ));
        }
    }

    protected function setInitialPosition(?string $position): void
    {
        if (empty($position)) {
            $this->logger->info('Got no former position, fetching full stream');
            $this->position = '0';
        } else {
            $this->logger->info("Resuming stream from $position");
            $this->position = $position;
        }
    }

    protected function readNextBatch(): ?array
    {
        $blockMs = $this->startingUp ? '25' : '10000';
        return $this->fetchBatchFromStream($this->position, 1000, $blockMs);
    }

    protected function fetchBatchFromStream($position, $maxCount, $blockMs)
    {
        return $this->redis->execute(
            'XREAD',
            'COUNT',
            (string) $maxCount,
            'BLOCK',
            (string) $blockMs,
            'STREAMS',
            $this->prefix . 'stream',
            $position,
        );
    }

    protected function logStartupInfo(): void
    {
        if ($this->backlogLinesAtStartup === 0) {
            $this->logger->info('Queue was empty at startup');
            return;
        }
        $duration = microtime(true) - $this->startupTime;
        $this->logger->info(sprintf(
            'Queue is now empty, processed %d lines (%s) in %s after starting up',
            $this->backlogLinesAtStartup,
            Format::bytes($this->backlogBytesAtStartup),
            $duration > 1 ? sprintf('%.2Fs', $duration) : sprintf('%.2Fms', $duration * 1000)
        ));
    }
}
