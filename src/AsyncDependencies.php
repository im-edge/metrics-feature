<?php

namespace IcingaMetrics;

use Psr\Log\LoggerInterface;
use React\EventLoop\LoopInterface;
use React\Promise\Deferred;
use React\Promise\ExtendedPromiseInterface;

class AsyncDependencies
{
    /** @var LoopInterface */
    protected $loop;

    /** @var LoggerInterface */
    protected $logger;

    public function __construct(
        LoggerInterface $logger
    ) {
        $this->logger = $logger;
    }

    public static function waitFor($subject, array $promises, $timeout, LoopInterface $loop, LoggerInterface $logger)
    {
        $deferred = new Deferred();
        $ready = [];
        $results = [];
        $timer = $loop->addTimer($timeout, function () use ($subject, $logger, $promises, &$ready) {
            $logger->info("$subject is still waiting for: " . implode(', ', array_diff(array_keys($promises), array_keys($ready))));
        });
        $done = static function () use ($deferred, $timer, &$ready, &$results, $subject, $loop, $logger) {
            foreach ($ready as $name => $isReady) {
                if (! $isReady) {
                    return;
                }
            }

            $loop->cancelTimer($timer);
            $logger->notice("$subject is ready");
            $deferred->resolve($results);
        };
        foreach ($promises as $name => $promise) {
            echo "Waiting for $name\n";
            assert($promise instanceof ExtendedPromiseInterface);
            $ready[$name] = false;
            $promise->then(function ($result) use (&$ready, &$results, $name, $done) {
                $results[$name] = $result;
                echo "$name is done\n";
                $ready[$name] = true;
                $done();
            }, function (\Throwable $e) use ($deferred) {
                echo $e->getMessage() . "\n";
                $deferred->reject($e);
            });
        }

        return $deferred->promise();
    }
}
