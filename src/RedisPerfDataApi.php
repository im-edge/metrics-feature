<?php

namespace iPerGraph;

use Clue\React\Redis\Client as RedisClient;
use Clue\React\Redis\Factory as RedisFactory;
use Exception;
use gipfl\ReactUtils\RetryUnless;
use gipfl\RedisLua\LuaScriptRunner;
use gipfl\RedisLua\RedisUtil;
use Psr\Log\LoggerInterface;
use React\EventLoop\LoopInterface;
use React\Promise\Deferred;
use React\Promise\ExtendedPromiseInterface;
use function React\Promise\reject;
use function React\Promise\resolve;

class RedisPerfDataApi
{
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

    public function __construct(LoopInterface $loop, LoggerInterface $logger, $redisConfig)
    {
        $this->loop = $loop;
        $this->logger = $logger;
        $this->socketUri = 'redis+unix://' . $redisConfig->socket;
        $this->luaDir = $redisConfig->luaDir;
    }

    /**
     * @return ExtendedPromiseInterface
     */
    public function getRedisConnection()
    {
        if ($this->redis === null) {
            $deferred = new Deferred();
            $this->redis = $deferred;
            $this->keepConnectingToRedis()->then(function (RedisClient $client) use ($deferred) {
                $this->redis = $client;
                $deferred->resolve($client);
            });
            return $deferred->promise();
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
        $retry = RetryUnless::succeeding(function () {
            return $this->connectToRedis();
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
            }, function (\Exception $e) {
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

    protected function redisIsReady(RedisClient $client)
    {
        $client->on('end', function () {
            $this->logger->info('Redis ended');
        });
        $client->on('error', function (\Exception $e) {
            $this->logger->error('Redis error: ' . $e->getMessage());
        });

        $client->on('close', function () {
            $this->logger->info('Redis closed');
        });

        $name = 'iPerGraph';
        $this->redis = $client;

        if ($name === null) {
            return resolve($client);
        }

        $deferred = new Deferred();
        $this->logger->info("Setting name to '$name'");
        $client->client('setname', $name)->then(function () use ($deferred) {
            $deferred->resolve($this->redis);
        })->otherwise(function (\Exception $e) use ($deferred) {
            $deferred->reject(sprintf('Unable to set my name: %s', $e->getMessage()));
        });

        return $deferred->promise();
    }
}
