<?php

namespace IMEdge\MetricsFeature;

use Psr\Log\LoggerInterface;
use React\EventLoop\Loop;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;

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
        $logList = implode(', ', array_keys($promises));

        $done = static function () use ($deferred, $timer, &$ready, &$results, $subject, $logger, $logList) {
            foreach ($ready as $name => $isReady) {
                if (! $isReady) {
                    return;
                }
            }

            Loop::get()->cancelTimer($timer);
            $logger->notice("$subject is ready to run, successfully waited for $logList");
            $deferred->resolve($results);
        };
        foreach ($promises as $name => $promise) {
            $logger->info("Waiting for $name");
            assert($promise instanceof PromiseInterface);
            $ready[$name] = false;
            $promise->then(function ($result) use (&$ready, &$results, $name) {
                $results[$name] = $result;
                $ready[$name] = true;
            })->then($done, function (\Throwable $e) use ($deferred, $name, $logger) {
                $logger->error('Failed waiting for ' . $name . ': ' . $e->getMessage());
                $deferred->reject($e);
            });
        }

        return $deferred->promise();
    }
}
