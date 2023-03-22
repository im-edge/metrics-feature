<?php

namespace IcingaMetrics;

use gipfl\RrdTool\AsyncRrdtool;
use gipfl\RrdTool\RrdCached\Client as RrdCachedClient;
use IcingaMetrics\Receiver\ReceiverRunner;
use IcingaMetrics\Redis\RedisRunner;
use IcingaMetrics\RrdCached\RrdCachedRunner;
use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidInterface;
use React\EventLoop\Loop;
use RuntimeException;

class MetricStore implements ProcessWithPidInterface
{
    use DirectoryBasedComponent;

    const CONFIG_TYPE = 'IcingaMetrics/MetricStore';
    const CONFIG_FILE_NAME = 'metric-store.json';
    const CONFIG_VERSION = 'v1';
    const SUPPORTED_CONFIG_VERSIONS = [
        self::CONFIG_VERSION,
    ];
    const SOCKET_PATH = '/run/icinga-metrics';
    const DEFAULT_REDIS_BINARY = '/usr/bin/redis-server';
    const DEFAULT_RRD_TOOL_BINARY = '/usr/bin/rrdtool';
    const DEFAULT_RRD_CACHED_BINARY = '/usr/bin/rrdcached';

    protected ?RedisRunner $redisRunner = null;
    protected RrdCachedRunner $rrdCachedRunner;

    public function getDataNodeUuid() : ?UuidInterface
    {
        $uuid = $this->config->get('datanode');
        if ($uuid) {
            return Uuid::fromString($uuid);
        }

        return null;
    }

    public function setDataStoreUuid(UuidInterface $uuid, bool $force = false)
    {
        $currentUuid = $this->getDataNodeUuid();
        if ($currentUuid) {
            if ($currentUuid->equals($uuid)) {
                return;
            }
            if (!$force) {
                throw new RuntimeException(sprintf(
                    'Cannot claim Metric Store "%s" for %s, it belongs to %s',
                    $this->getName(),
                    $uuid->toString(),
                    $currentUuid->toString()
                ));
            }
        }

        $this->config->set('datanode', $uuid->toString());
        $this->storeConfig($this->config);
    }

    public function getRedisSocketUri(): string
    {
        return $this->redisRunner()->getSocketUri();
    }

    protected function redisRunner(): RedisRunner
    {
        if ($this->redisRunner === null) {
            $this->redisRunner = new RedisRunner(static::getRedisBinary(), $this->baseDir . '/redis', $this->logger);
        }

        return $this->redisRunner;
    }

    protected function initialize()
    {
        $this->redisRunner()->run();
        $this->rrdCachedRunner = new RrdCachedRunner(
            static::getRrdCacheDBinary(),
            $this->baseDir . '/rrdcached',
            $this->logger
        );
        $this->rrdCachedRunner->run();
        chdir($this->rrdCachedRunner->getDataDir());
        $this->runSelfMonitoring();
        Loop::get()->addTimer(1, function () {
            $this->runDeferredHandler();
            $this->runMainHandler();
        });
        Loop::addTimer(2, function () {
            $this->initializeRemoteApi();
        });
        Loop::addTimer(3, function () {
            if ($receivers = $this->config->get('receivers')) {
                $runner = new ReceiverRunner($this->logger, $receivers, $this);
                $runner->run();
            }
        });
    }

    protected function initializeRemoteApi()
    {
        $rrdtool = new AsyncRrdtool(
            $this->rrdCachedRunner->getDataDir(),
            static::getRrdToolBinary(),
            $this->rrdCachedRunner->getSocketFile()
        );
        $rrdtool->setLogger($this->logger);
        $rrdCached = new RrdCachedClient($this->rrdCachedRunner->getSocketFile(), Loop::get());

        $api = new MetricStoreRemoteApi($this->logger, $rrdtool, $rrdCached);
        $api->run(self::SOCKET_PATH . '/' . $this->getUuid()->toString() . '.sock');
    }

    protected function runMainHandler()
    {
        $redis = new RedisPerfDataApi($this->logger, $this->redisRunner->getSocketUri());
        $this->logger->info('MainHandler connecting to redis via ' . $this->redisRunner->getSocketUri());
        $redis->setClientName(Application::PROCESS_NAME . '::main');
        $rrdCached = new RrdCachedClient($this->rrdCachedRunner->getSocketFile(), Loop::get());
        $mainHandler = new MainUpdateHandler($redis, $rrdCached, $this->logger);
        $mainHandler->run();
    }

    protected function runDeferredHandler()
    {
        $redis = new RedisPerfDataApi($this->logger, $this->redisRunner->getSocketUri());
        $this->logger->info('DeferredHandler connecting to redis via ' . $this->redisRunner->getSocketUri());
        $redis->setClientName(Application::PROCESS_NAME . '::deferred');
        $rrdCached = new RrdCachedClient($this->rrdCachedRunner->getSocketFile(), Loop::get());
        $rrdCached->setLogger($this->logger);
        $rrdtool = new AsyncRrdtool(
            $this->rrdCachedRunner->getDataDir(),
            static::getRrdToolBinary()
        );
        $rrdtool->setLogger($this->logger);
        $deferredHandler = new DeferredHandler($redis, $rrdCached, $rrdtool, $this->logger);
        $deferredHandler->run();
    }

    protected function runSelfMonitoring()
    {
        $redis = new RedisPerfDataApi($this->logger, $this->redisRunner->getSocketUri());
        $redis->setClientName(Application::PROCESS_NAME . '::self-monitoring');
        $rrdCached = new RrdCachedClient($this->rrdCachedRunner->getSocketFile(), Loop::get());
        $rrdCached->setLogger($this->logger);
        $monitor = new SelfMonitoring(
            $redis,
            $rrdCached,
            $this->logger,
            [
                'redis-server' => $this->redisRunner,
                'rrdcached'    => $this->rrdCachedRunner,
                'metric-store' => $this,
            ],
            $this->getUuid()->toString()
        );
        $monitor->on(RedisPerfDataApi::ON_PERF_DATA, [$redis, 'shipPerfData']);
        $monitor->run(15);
    }

    public function getProcessPid(): ?int
    {
        return getmypid();
    }

    protected static function getRedisBinary(): string
    {
        return static::DEFAULT_REDIS_BINARY;
    }

    protected static function getRrdCacheDBinary(): string
    {
        return static::DEFAULT_RRD_CACHED_BINARY;
    }

    protected static function getRrdToolBinary(): string
    {
        return static::DEFAULT_RRD_TOOL_BINARY;
    }
}
