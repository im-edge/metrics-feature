<?php

namespace iPerGraph;

use Clue\React\Redis\Client as RedisClient;
use Clue\React\Redis\Factory as RedisFactory;
use Exception;
use gipfl\ReactUtils\RetryUnless;
use gipfl\RedisUtils\LuaScriptRunner;
use gipfl\RedisUtils\RedisUtil;
use Psr\Log\LoggerInterface;
use React\EventLoop\LoopInterface;
use React\Promise\Deferred;
use React\Promise\ExtendedPromiseInterface;
use function React\Promise\reject;
use function React\Promise\resolve;
use function current;
use function json_encode;
use function time;

class RedisPerfDataApi
{
    const JSON_FLAGS = JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRESERVE_ZERO_FRACTION;

    /** @var LoopInterface */
    protected $loop;

    /** @var LoggerInterface */
    protected $logger;

    /** @var ExtendedPromiseInterface|RedisClient */
    protected $redis;

    /** @var string */
    protected $socketUri;

    /** @var string */
    protected $luaDir;

    /** @var LuaScriptRunner */
    protected $lua;

    protected $prefix = 'rrd:';

    protected $clientName = 'iPerGraph';

    public function __construct(LoopInterface $loop, LoggerInterface $logger, $redisConfig)
    {
        $this->loop = $loop;
        $this->logger = $logger;
        $this->socketUri = 'redis+unix://' . $redisConfig->socket;
        $this->luaDir = $redisConfig->luaDir;
    }

    public function setClientName($name)
    {
        $this->clientName = $name;
        if ($this->redis instanceof RedisClient) {
            $this->redis->client('setname', $this->clientName);
        }

        return $this;
    }

    /**
     * @return ExtendedPromiseInterface
     */
    public function getRedisConnection()
    {
        if ($this->redis === null) {
            $deferred = new Deferred();
            $this->redis = $deferred->promise();
            $this->keepConnectingToRedis()->then(function (RedisClient $client) use ($deferred) {
                $this->redis = $client;
                $deferred->resolve($client);
            });
            return $this->redis;
        }

        return resolve($this->redis);
    }

    /**
     * @return ExtendedPromiseInterface
     */
    public function getLuaRunner()
    {
        if ($this->lua === null) {
            $deferred = new Deferred();
            $this->getRedisConnection()->then(function (RedisClient $client) use ($deferred) {
                $lua = new LuaScriptRunner($client, $this->luaDir);
                $lua->setLogger($this->logger);
                $this->lua = $lua;
                $deferred->resolve($lua);
            });
        }

        return resolve($this->lua);
    }

    public function shipPerfData(PerfData $perfData)
    {
        return $this->getLuaRunner()->then(function (LuaScriptRunner $lua) use ($perfData) {
            $lua->runScript('shipPerfData', [$perfData->toJson()]) // TODO: ship prefix
            ->then(function ($result) {
                return RedisUtil::makeHash($result);
            }, function (Exception $e) {
                $this->logger->error($e->getMessage());
                return $e;
            });
        });
    }

    protected function keepConnectingToRedis()
    {
        $deferred = new Deferred();
        $retry = RetryUnless::succeeding(function () use ($deferred) {
            return $this->connectToRedis()->then(function (RedisClient $client) use ($deferred) {
                $deferred->resolve($client);
            });
        })->slowDownAfter(10, 5);
        $retry->setLogger($this->logger);
        $retry->run($this->loop);

        return $deferred->promise();
    }

    public function connectToRedis()
    {
        $factory = new RedisFactory($this->loop);
        return $factory
            ->createClient($this->socketUri)
            ->then(function (RedisClient $client) {
                return $this->redisIsReady($client);
            }, function (Exception $e) {
                if ($previous = $e->getPrevious()) {
                    throw new \RuntimeException($e->getMessage() . ': ' . $e->getPrevious()->getMessage(), 0, $e);
                }

                throw $e;
            });
    }

    public function getCounters()
    {
        if ($this->redis instanceof RedisClient) {
            return $this->redis->hgetall($this->prefix . 'counters')
                ->then(function ($result) {
                    if (empty($result)) {
                        return reject();
                    }
                    return RedisUtil::makeHash($result);
                });
        }

        return reject();
    }

    public function fetchBatchFromStream($position, $maxCount, $blockMs)
    {
        return $this->getRedisConnection()->then(function (RedisClient $client) use ($position, $maxCount, $blockMs) {
            return $client->xread(
                'COUNT',
                (string) $maxCount,
                'BLOCK',
                (string) $blockMs,
                'STREAMS',
                $this->prefix . 'stream',
                $position
            );
        });
    }

    public function fetchLastPosition()
    {
        return $this->getRedisConnection()->then(function ($client) {
            return $client->get($this->prefix . 'stream-last-pos');
        });
    }

    public function setLastPosition($position)
    {
        return $this->getRedisConnection()->then(function (RedisClient $client) use ($position) {
            return $client->set($this->prefix . 'stream-last-pos', $position);
        });
    }

    public function setCiConfig($ci, $config)
    {
        $this->logger->debug("Registering $ci in Redis");
        return $this->hSet('ci', $ci, json_encode($config, self::JSON_FLAGS));
    }

    public function deferCi($ci, $reason = null)
    {
        if ($reason === null) {
            $value = time();
        } else {
            $value = json_encode([
                'reason' => $reason,
                'since'  => time()
            ]);
        }
        return $this->hSet('deferred-cids', $ci, $value);
    }

    public function getFirstDeferredPerfDataForCi($ci)
    {
        $key = $this->prefix . "deferred:$ci";
        return $this->getRedisConnection()
            ->then(function (RedisClient $client) use ($ci, $key) {
                return $client->xlen($key)->then(function ($count) use ($ci, $key, $client) {
                    if ($count === 0) {
                        $this->logger->debug("DeferredHandler: there are no pending values for $ci");
                        return null;
                    }

                    $this->logger->debug("DeferredHandler: there are $count values for $ci, getting first one");
                    return $client->xrange($key, '-', '+', 'COUNT', '1');
                });
            })->then(function ($streamResult) use ($ci) {
                if ($streamResult === null) {
                    $this->logger->info("'$ci' was deferred, but had no performance data. Freeing.");
                    $this->rescheduleDeferredCi($ci);
                    return null;
                }

                // We fetched only one row
                return PerfData::fromJson(current($streamResult)[1][1]);
            });
    }

    public function rescheduleDeferredCi($ci)
    {
        return $this->getLuaRunner()->then(function (LuaScriptRunner $lua) use ($ci) {
            return $lua->runScript('rescheduleDeferredCi', [$ci]);
        });
    }

    public function fetchDeferred()
    {
        return $this->getHash('deferred-ci');
    }

    protected function hSet($hash, $key, $value)
    {
        return $this->getRedisConnection()->then(function (RedisClient $client) use ($hash, $key, $value) {
            return $client->hset($this->prefix . $hash, $key, $value);
        });
    }

    protected function getHash($key)
    {
        return $this->getRedisConnection()->then(function (RedisClient $client) use ($key) {
            return $client->hgetall($this->prefix . $key)->then([RedisUtil::class, 'makeHash']);
        });
    }

    protected function redisIsReady(RedisClient $client)
    {
        $client->on('end', function () {
            $this->logger->info('Redis ended');
        });
        $client->on('error', function (Exception $e) {
            $this->logger->error('Redis error: ' . $e->getMessage());
        });

        $client->on('close', function () {
            $this->logger->info('Redis closed');
        });

        $this->redis = $client;

        return $client->client('setname', $this->clientName)->then(function () {
            return $this->redis;
        });
    }
}
