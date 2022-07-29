<?php

namespace IcingaMetrics;

use Exception;
use gipfl\RrdTool\RrdCached\Client as RrdCachedClient;
use Psr\Log\LoggerInterface;
use React\EventLoop\Loop;
use React\Promise\ExtendedPromiseInterface;
use RuntimeException;

class MainUpdateHandler
{
    /** @var RedisPerfDataApi */
    protected $redisApi;

    /** @var LoggerInterface */
    protected $logger;

    /** @var RrdCachedClient */
    protected $rrdCached;

    protected $position;

    protected $startupTime;

    protected $startingUp = true;

    protected $shuttingDown = false;

    protected $backlogLinesAtStartup = 0;

    protected $backlogBytesAtStartup = 0;

    protected $funcFetchNext;

    /**
     * @param RedisPerfDataApi $redisApi
     * @param RrdCachedClient $rrdCached
     * @param LoggerInterface $logger
     */
    public function __construct(RedisPerfDataApi $redisApi, RrdCachedClient $rrdCached, LoggerInterface $logger)
    {
        $this->redisApi = $redisApi;
        $this->rrdCached = $rrdCached;
        $this->logger = $logger;
        $this->funcFetchNext = function () {
            $this->processNextScheduledBatch();
        };
    }

    public function run()
    {
        $this->startupTime = microtime(true);
        $this->logger->info("MainUpdateHandler is starting");
        $this->fetchLastPosition()->then($this->funcFetchNext);
    }

    protected function fetchLastPosition(): ExtendedPromiseInterface
    {
        return $this->redisApi->fetchLastPosition()->then(function ($position) {
            $this->setInitialPosition($position);
        });
    }

    protected function processNextScheduledBatch()
    {
        if ($this->shuttingDown) {
            return;
        }

        $this->readNextBatch()->then(function ($stream) {
            $this->processBulk($stream);
        });
    }

    protected function scheduleNextFetch()
    {
        Loop::get()->futureTick($this->funcFetchNext);
    }

    /**
     * @param $stream
     */
    protected function processBulk($stream)
    {
        if (empty($stream)) {
            if ($this->startingUp) {
                $this->logStartupInfo();
                $this->startingUp = false;
            }

            $this->scheduleNextFetch();

            return;
        }

        $bulk = '';

        // $stream looks like this:
        // [0] => [ 'rrd:stream', [ 0 => .. ]
        $stream = $stream[0][1] ?? null;
        if (empty($stream)) {
            if ($this->startingUp) {
                $this->startingUp = false;
            }
            $this->logger->warning('Got an unexpected stream construct, please let us know');

            $this->scheduleNextFetch();
            return;
        }

        foreach ($stream as $entry) {
            $this->position = $entry[0];
            // entry = [ 1537893299864-0, [key, val, .., ..]
            if ($entry[1][0] !== 'update') {
                throw new RuntimeException("Got invalid update from stream: " . json_encode($entry, 1));
            }
            $line = $entry[1][1];
            if ($this->startingUp) {
                $this->backlogLinesAtStartup++;
                $this->backlogBytesAtStartup += strlen($line) + 1;
            }
            $bulk .= "update $line\n";
        }
        if ($bulk === '') {
            $this->logger->notice('Nothing to send with this batch, pretty strange');
            $this->scheduleNextFetch();
            return;
        }

        $this->rrdCached->batch($bulk)
            ->then(function ($result) use ($stream) {
                if ($result !== true) {
                    $this->processErrors($result, $stream);
                }

                return $this->redisApi->setLastPosition($this->position);
            })->otherwise(function (Exception $e) {
                $this->logger->error($e->getMessage());
            })->always(function () {
                $this->scheduleNextFetch();
            });
    }

    /**
     * Special treatment for step errors, as there can potentially be LOTS of
     * them
     *
     * @param $result
     * @param $stream
     */
    protected function processErrors($result, $stream)
    {
        $stepError = 0;
        // TODO: $stepErrorFiles = [];
        foreach ($result as $pos => $error) {
            // rrdCached starts counting with 1
            $realPos = $pos - 1;
            $failedLine = $stream[$realPos][1][1] ?? '<unknown line>';

            if (strpos($error, 'No such file') !== false) {
                $filename = substr($failedLine, 0, strpos($failedLine, ' '));
                // $this->redisApi->deferCi()
                $this->logger->debug(sprintf(
                    'RRDCacheD rejected for missing file "%s": %s',
                    $failedLine,
                    $error
                ));
                // $this->redisApi->deferCi()
            } elseif (strpos($error, 'minimum one second step') !== false) {
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

    protected function setInitialPosition($position)
    {
        if (empty($position)) {
            $this->logger->info('Got no former position, fetching full stream');
            $this->position = '0';
        } else {
            $this->logger->info("Resuming stream from $position");
            $this->position = $position;
        }
    }

    protected function readNextBatch(): ExtendedPromiseInterface
    {
        $blockMs = $this->startingUp ? '25' : '1000';
        return $this->redisApi->fetchBatchFromStream($this->position, 1000, $blockMs);
    }

    protected function logStartupInfo()
    {
        if ($this->backlogLinesAtStartup > 0) {
            $duration = microtime(true) - $this->startupTime;
            if ($duration > 1) {
                $duration = sprintf('%.2Fs', $duration);
            } else {
                $duration = sprintf('%.2Fms', $duration * 1000);
            }
            $this->logger->info(sprintf(
                'Queue is now empty, processed %d lines (%s) in %s after starting up',
                $this->backlogLinesAtStartup,
                CompactFormat::bytes($this->backlogBytesAtStartup),
                $duration
            ));
        } else {
            $this->logger->info('Queue was empty at startup');
        }
    }
}
