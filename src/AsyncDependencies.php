<?php

namespace IcingaMetrics;

use Psr\Log\LoggerInterface;
use React\EventLoop\Loop;
use React\Promise\Deferred;
use React\Promise\ExtendedPromiseInterface;

class AsyncDependencies
{
    public static function waitFor($subject, array $promises, $timeout, LoggerInterface $logger)
    {
        $deferred = new Deferred();
        $ready = [];
        $results = [];
        $timer = Loop::get()->addTimer($timeout, function () use ($subject, $promises, &$ready, $logger) {
            $diff = array_diff(array_keys($promises), array_keys($ready));
            if (! empty($diff)) {
                $logger->info("$subject is still waiting for: " . implode(', ', $diff));
            }
        });

        $done = static function () use ($deferred, $timer, &$ready, &$results, $subject, $logger) {
            foreach ($ready as $name => $isReady) {
                if (! $isReady) {
                    return;
                }
            }

            Loop::get()->cancelTimer($timer);
            $logger->notice("$subject is ready");
            $deferred->resolve($results);
        };
        foreach ($promises as $name => $promise) {
            $logger->info("Waiting for $name");
            assert($promise instanceof ExtendedPromiseInterface);
            $ready[$name] = false;
            $promise->then(function ($result) use (&$ready, &$results, $name) {
                $results[$name] = $result;
                $ready[$name] = true;
            })->then($done, function (\Throwable $e) use ($deferred) {
                echo $e->getMessage() . "\n";
                $deferred->reject($e);
            });
        }

        return $deferred->promise();
    }
}
