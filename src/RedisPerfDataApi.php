<?php

namespace IcingaMetrics;

use Clue\React\Redis\Client as RedisClient;
use Clue\React\Redis\Factory as RedisFactory;
use Evenement\EventEmitterInterface;
use Evenement\EventEmitterTrait;
use Exception;
use gipfl\Json\JsonString;
use gipfl\ReactUtils\RetryUnless;
use gipfl\RedisUtils\LuaScriptRunner;
use gipfl\RedisUtils\RedisUtil;
use gipfl\RrdTool\DsList;
use gipfl\RrdTool\RraSet;
use Psr\Log\LoggerInterface;
use React\EventLoop\Loop;
use React\Promise\Deferred;
use React\Promise\ExtendedPromiseInterface;
use function React\Promise\all;
use function React\Promise\reject;
use function React\Promise\resolve;
use function time;

class RedisPerfDataApi implements EventEmitterInterface
{
    use EventEmitterTrait;

    const ON_PERF_DATA = 'perfData';
    const ON_STRAIN_START = 'strain_start';

    const ON_STRAIN_END = 'strain_end';

    const STRAIN_START = 100000;

    const STRAIN_END = 5000;

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

    protected $clientName = 'IcingaGraphing';

    protected $cntPending = 0;

    protected $isStrain = false;

    public function __construct(LoggerInterface $logger, $redisSocketUri)
    {
        $this->logger = $logger;
        $this->socketUri = $redisSocketUri;
        $this->luaDir = dirname(__DIR__) . '/lua';
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
    public function getRedisConnection(): ExtendedPromiseInterface
    {
        if ($this->redis === null) {
            $this->logger->debug('Initiating a new Redis connection');
            $deferred = new Deferred();
            $this->redis = $deferred->promise();
            $this->keepConnectingToRedis()->then(function (RedisClient $client) use ($deferred) {
                $this->redis = $client;
                $deferred->resolve($client);
            });
        }
        if (! $this->redis instanceof RedisClient) {
            $this->logger->info('Redis is still a ' . get_class($this->redis));
        }

        return resolve($this->redis);
    }

    /**
     * @return ExtendedPromiseInterface
     */
    public function getLuaRunner(): ExtendedPromiseInterface
    {
        if ($this->lua === null) {
            $deferred = new Deferred();
            $this->lua = $deferred->promise();
            $this->getRedisConnection()->then(function (RedisClient $client) use ($deferred) {
                $lua = new LuaScriptRunner($client, $this->luaDir);
                $lua->setLogger($this->logger);
                $this->lua = $lua;
                $deferred->resolve($lua);
            });
        }

        return resolve($this->lua);
    }

    protected function incPending($count = 1)
    {
        $this->cntPending += $count;
        // $this->logger->debug('Pending: ' . $this->cntPending . ' after ' . $count);
        if ($this->isStrain) {
            if ($this->cntPending < self::STRAIN_END) {
                $this->isStrain = false;
                $this->emit(self::ON_STRAIN_END, [$this->cntPending]);
            }
        } else {
            if ($this->cntPending >= self::STRAIN_START) {
                $this->isStrain = true;
                $this->emit(self::ON_STRAIN_START, [$this->cntPending]);
            }
        }
    }

    public function shipPerfData(PerfData $perfData): ExtendedPromiseInterface
    {
        $this->incPending();
        return $this->getLuaRunner()->then(function (LuaScriptRunner $lua) use ($perfData) {
            $lua->runScript('shipPerfData', [JsonString::encode($perfData)]) // TODO: ship prefix
            ->then([RedisUtil::class, 'makeHash'])->then(function ($result) {
                $this->incPending(-1);
                return $result;
            }, function (Exception $e) {
                $this->incPending(-1);
                $this->logger->error($e->getMessage());
                return $e;
            });
        });
    }

    /**
     * @param PerfData[] $perfDataList
     * @return ExtendedPromiseInterface
     */
    public function shipBulkPerfData(array $perfDataList): ExtendedPromiseInterface
    {
        $count = count($perfDataList);
        $this->incPending($count);
        return $this->getLuaRunner()->then(function (LuaScriptRunner $lua) use ($perfDataList, $count) {
            // TODO: ship prefix?!
            $lua->runScript('shipBulkPerfData', array_map([JsonString::class, 'encode'], $perfDataList))
            ->then([RedisUtil::class, 'makeHash'])->then(function ($result) use ($count) {
                $this->incPending(-$count);
                return $result;
            }, function (Exception $e) use ($count) {
                $this->incPending(-$count);
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
        $retry->run(Loop::get());

        return $deferred->promise();
    }

    public function connectToRedis()
    {
        $factory = new RedisFactory(Loop::get());
        return $factory
            ->createClient($this->socketUri)
            ->then(function (RedisClient $client) {
                return $this->redisIsReady($client);
            }, function (Exception $e) {
                $this->logger->error('Connection error: ' . $e->getMessage());
                if ($previous = $e->getPrevious()) {
                    throw new \RuntimeException($e->getMessage() . ': ' . $previous->getMessage(), 0, $e);
                }

                throw $e;
            });
    }

    public function getCounters()
    {
        return $this->getRedisConnection()->then(function (RedisClient $redis) {
            return $redis->hgetall($this->prefix . 'counters');
        })->then(function ($result) {
            if (empty($result)) {
                return reject(new Exception('Redis currently has no counters'));
            }
            return RedisUtil::makeHash($result);
        });
    }

    public function readFromStream($stream, $position, $maxCount, $blockMs): ExtendedPromiseInterface
    {
        return $this->getRedisConnection()
            ->then(function (RedisClient $client) use ($position, $maxCount, $blockMs, $stream) {
                return $client->xread(
                    'COUNT',
                    (string) $maxCount,
                    'BLOCK',
                    (string) $blockMs,
                    'STREAMS',
                    $stream,
                    $position
                );
            });
    }

    public function fetchBatchFromStream($position, $maxCount, $blockMs): ExtendedPromiseInterface
    {
        return $this->readFromStream($this->prefix . 'stream', $position, $maxCount, $blockMs);
    }

    public function fetchLastPosition(): ExtendedPromiseInterface
    {
        return $this->getRedisConnection()->then(function ($client) {
            return $client->get($this->prefix . 'stream-last-pos');
        });
    }

    public function setLastPosition($position): ExtendedPromiseInterface
    {
        return $this->getRedisConnection()->then(function (RedisClient $client) use ($position) {
            return $client->set($this->prefix . 'stream-last-pos', $position);
        });
    }

    public function fetchBatchFromCiUpdateStream($position, $maxCount, $blockMs): ExtendedPromiseInterface
    {
        return $this->readFromStream($this->prefix . 'ci-changes', $position, $maxCount, $blockMs);
    }

    public function fetchLastCiUpdatePosition(): ExtendedPromiseInterface
    {
        return $this->getRedisConnection()->then(function ($client) {
            return $client->get($this->prefix . 'ci-stream-last-pos');
        });
    }

    public function setLastCiUpdatePosition($position): ExtendedPromiseInterface
    {
        return $this->getRedisConnection()->then(function (RedisClient $client) use ($position) {
            return $client->set($this->prefix . 'ci-stream-last-pos', $position);
        });
    }

    public function setCiConfig($ci, CiConfig $config, DsList $dsList, RraSet $rraSet)
    {
        $this->logger->debug("Registering $ci in Redis");
        $json =  JsonString::encode($config);
        return all([
            $this->xAdd(
                'ci-changes',
                'MAXLEN',
                '~',
                500000,
                '*',
                'ci',
                $ci,
                'config',
                $json,
                'ds',
                (string) $dsList,
                'rra',
                (string) $rraSet
            ),
            $this->hSet('ci', $ci, $json),
        ]);
    }

    public function deferCi($ci, $reason = null)
    {
        if ($reason === null) {
            $value = time();
        } else {
            $value = JsonString::encode([
                'reason' => $reason,
                'since'  => time()
            ]);
        }
        return $this->hSet('deferred-cids', $ci, $value);
    }

    public function rescheduleDeferredCi($ci): ExtendedPromiseInterface
    {
        return $this->getLuaRunner()->then(function (LuaScriptRunner $lua) use ($ci) {
            return $lua->runScript('rescheduleDeferredCi', [$ci]);
        });
    }

    public function fetchDeferred(): ExtendedPromiseInterface
    {
        return $this->fetchSetAsArray('deferred-ci');
    }

    protected function hSet($hash, $key, $value): ExtendedPromiseInterface
    {
        return $this->getRedisConnection()->then(function (RedisClient $client) use ($hash, $key, $value) {
            return $client->hset($this->prefix . $hash, $key, $value);
        });
    }

    protected function xAdd($stream, ...$args): ExtendedPromiseInterface
    {
        return $this->getRedisConnection()->then(function (RedisClient $client) use ($stream, $args) {
            return $client->xadd($this->prefix . $stream, ...$args);
        });
    }

    protected function fetchSetAsArray($key): ExtendedPromiseInterface
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
            $this->redis = null;
            $this->logger->error('Redis error: ' . $e->getMessage());
        });

        $client->on('close', function () {
            $this->redis = null;
            $this->logger->info('Redis closed');
        });

        $this->redis = $client;

        return $client->client('setname', $this->clientName)->then(function () {
            return $this->redis;
        });
    }
}
