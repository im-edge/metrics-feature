<?php

namespace IcingaMetrics;

use gipfl\DataType\Settings;
use gipfl\RrdTool\AsyncRrdtool;
use gipfl\RrdTool\RrdCached\Client as RrdCachedClient;
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
            $this->redisRunner = new RedisRunner('/usr/bin/redis-server', $this->baseDir . '/redis', $this->logger);
        }

        return $this->redisRunner;
    }

    protected function initialize()
    {
        $this->redisRunner()->run();
        $this->rrdCachedRunner = new RrdCachedRunner(
            '/usr/bin/rrdcached',
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
    }

    protected function initializeRemoteApi()
    {
        $rrdtool = new AsyncRrdtool(
            $this->rrdCachedRunner->getDataDir(),
            '/usr/bin/rrdtool',
            $this->rrdCachedRunner->getSocketFile()
        );
        $rrdtool->setLogger($this->logger);
        $rrdCached = new RrdCachedClient($this->rrdCachedRunner->getSocketFile(), Loop::get());

        $api = new RemoteApi($this->logger, $rrdtool, $rrdCached);
        $api->run('/run/icinga-metrics/' . $this->getUuid()->toString() . '.sock');
    }

    protected function runMainHandler()
    {
        $redis = new RedisPerfDataApi($this->logger, $this->redisRunner->getSocketUri());
        $redis->setClientName('IcingaMetrics::main');
        $rrdCached = new RrdCachedClient($this->rrdCachedRunner->getSocketFile(), Loop::get());
        $mainHandler = new MainUpdateHandler($redis, $rrdCached, $this->logger);
        $mainHandler->run();
    }

    protected function runDeferredHandler()
    {
        $redis = new RedisPerfDataApi($this->logger, $this->redisRunner->getSocketUri());
        $redis->setClientName('IcingaMetrics::deferred');
        $rrdCached = new RrdCachedClient($this->rrdCachedRunner->getSocketFile(), Loop::get());
        $rrdCached->setLogger($this->logger);
        $rrdtool = new AsyncRrdtool(
            $this->rrdCachedRunner->getDataDir(),
            '/usr/bin/rrdtool'
        );
        $rrdtool->setLogger($this->logger);
        $deferredHandler = new DeferredHandler($redis, $rrdCached, $rrdtool, $this->logger);
        $deferredHandler->run();
    }

    protected function runSelfMonitoring()
    {
        $redis = new RedisPerfDataApi($this->logger, $this->redisRunner->getSocketUri());
        $redis->setClientName('IcingaMetrics::self-monitoring');
        $rrdCached = new RrdCachedClient($this->rrdCachedRunner->getSocketFile(), Loop::get());
        $rrdCached->setLogger($this->logger);
        $monitor = new SelfMonitoring($redis, $rrdCached, $this->logger, $this->getUuid()->toString());
        $monitor->on(RedisPerfDataApi::ON_PERF_DATA, [$redis, 'shipPerfData']);
        $monitor->run(15);
    }

    public function getProcessPid(): ?int
    {
        return getmypid();
    }
}
