<?php

namespace IMEdge\MetricsFeature;

use Amp\Redis\RedisClient;
use gipfl\Json\JsonString;
use IMEdge\MetricsFeature\Api\StoreApi\MinimalNodeApi;
use IMEdge\MetricsFeature\Api\StoreApi\RrdApi;
use IMEdge\MetricsFeature\FileInventory\DeferredRedisTables;
use IMEdge\MetricsFeature\FileInventory\RedisTableStore;
use IMEdge\MetricsFeature\FileInventory\RrdFileStore;
use IMEdge\MetricsFeature\Receiver\ReceiverRunner;
use IMEdge\MetricsFeature\Rrd\RrdCachedRunner;
use IMEdge\MetricsFeature\Rrd\RrdtoolRunner;
use IMEdge\Node\Rpc\ApiRunner;
use IMEdge\ProcessRunner\ProcessWithPidInterface;
use IMEdge\RedisRunner\RedisRunner;
use IMEdge\RedisTables\RedisTables;
use IMEdge\RedisUtils\LuaScriptRunner;
use IMEdge\RedisUtils\RedisResult;
use IMEdge\RrdCached\RrdCachedClient;
use IMEdge\SimpleDaemon\DaemonComponent;
use Psr\Log\LoggerInterface;

use function Amp\async;
use function Amp\delay;
use function Amp\Future\awaitAll;
use function Amp\Redis\createRedisClient;

/**
 * Started per MetricStore, as a sub-process
 */
class MetricStoreRunner implements DaemonComponent, ProcessWithPidInterface
{
    public const ON_MEASUREMENTS = 'measurements';

    protected const DEFAULT_REDIS_BINARY = '/usr/bin/redis-server';
    protected const DEFAULT_RRD_TOOL_BINARY = '/usr/local/bin/rrdtool';
    protected const DEFAULT_RRD_CACHED_BINARY = '/usr/local/bin/rrdcached';

    protected RrdCachedRunner $rrdCachedRunner;
    protected RrdtoolRunner $rrdtoolRunner;
    protected RedisRunner $redisRunner;
    protected ?DeferredRedisTables $deferredHandler = null;
    protected ?MainUpdateHandler $mainHandler = null;
    protected ?SelfMonitoring $selfMonitoring = null;

    public function __construct(
        protected readonly MetricStore $metricStore,
        protected readonly RedisClient $mainRedis,
        protected readonly ApiRunner $api,
        protected readonly LoggerInterface $logger,
    ) {
        $this->rrdCachedRunner = new RrdCachedRunner(
            static::getRrdCacheDBinary(),
            $metricStore->getBaseDir() . '/rrdcached',
            $this->logger
        );
        $this->redisRunner = new RedisRunner(
            static::getRedisBinary(),
            $this->metricStore->getRedisBaseDir(),
            $this->logger
        );
        $this->rrdtoolRunner = $this->createRrdtoolRunner('de_DE.UTF8', $this->rrdCachedRunner->getSocketFile());
    }

    protected function createRrdtoolRunner(?string $locale = null, ?string $rrdCachedSocket = null): RrdtoolRunner
    {
        $runner = new RrdtoolRunner(
            static::getRrdToolBinary(),
            $this->rrdCachedRunner->getDataDirectory(),
            $this->logger
        );

        if ($locale) {
            $runner->setLocale('de_DE.UTF8');
        }
        if ($rrdCachedSocket) {
            $runner->setRrdCachedSocket($rrdCachedSocket);
        }

        return $runner;
    }

    public function start(): void
    {
        $metricStore = $this->metricStore;
        $metricStore->run();
        $this->api->addApi(new MinimalNodeApi($this->metricStore, $this->api, $this->logger));
        $this->redisRunner->run();
        chdir($this->rrdCachedRunner->getDataDirectory());
        $this->rrdCachedRunner->run();
        $this->rrdtoolRunner->run();
        delay(0.05);
        $this->runSelfMonitoring();
        $this->runDeferredHandler();
        $this->runMainHandler();
        $this->initializeRrdtool();
        delay(0.05);
        if ($receivers = $metricStore->requireConfig()->get('receivers')) {
            $runner = new ReceiverRunner($this->logger, $receivers, $metricStore);
            $runner->run();
        }
        $this->logger->notice('Metric store is ready: ' . $this->metricStore->getName());
    }

    public function stop(): void
    {

        $this->logger->notice('Stopping metric store ' . $this->metricStore->getName());
        $futures = [
            async($this->selfMonitoring->stop(...)),
            async($this->redisRunner->stop(...)),
            async($this->rrdCachedRunner->stop(...)),
            async($this->rrdtoolRunner->stop(...)),
        ];
        if ($this->mainHandler) {
            $futures[] = async($this->mainHandler->stop(...));
        }
        if ($this->deferredHandler) {
            $futures[] = async($this->deferredHandler->stop(...));
        }

        awaitAll($futures);
    }

    protected static function getRedisBinary(): string
    {
        return static::DEFAULT_REDIS_BINARY;
    }

    protected static function getRrdCacheDBinary(): string
    {
        return static::DEFAULT_RRD_CACHED_BINARY;
    }

    protected function initializeRrdtool(): void
    {
        $this->api->addApi(new RrdApi($this->rrdtoolRunner, $this->connectToRrdCached(), $this->logger));
    }

    protected function runMainHandler(): void
    {
        $this->mainHandler = new MainUpdateHandler(
            $this->connectToRedis('main'),
            $this->connectToRrdCached(),
            $this->logger
        );
        $this->mainHandler->run();
    }

    protected function connectToRedis(string $clientNameSuffix): RedisClient
    {
        $socket = $this->metricStore->getRedisSocketPath();
        $this->logger->info(sprintf('MainHandler::%s connecting to redis via %s', $clientNameSuffix, $socket));
        $client = createRedisClient('unix://' . $socket);
        $client->execute('CLIENT', 'SETNAME', ApplicationFeature::PROCESS_NAME . '::' . $clientNameSuffix);
        return $client;
    }

    protected function connectToRrdCached(): RrdCachedClient
    {
        // TODO: remember, disconnect/shutdown if required
        return new RrdCachedClient($this->rrdCachedRunner->getSocketFile());
    }

    protected function runDeferredHandler(): void
    {
        $redisClient = $this->connectToRedis('deferred');
        $rrdCached = $this->connectToRrdCached();
        $rrdCached->stats();
        $this->logger->notice('RrdCached is ready for DeferredRedisTables');
        $rrdtool = $this->createRrdtoolRunner('de_DE.UTF8', $this->rrdCachedRunner->getSocketFile());
        $rrdtool->run();
        $rrdFileStore = new RrdFileStore($rrdCached, $rrdtool, $this->logger);
        $store = new RedisTableStore($redisClient, $rrdCached, $rrdFileStore, $this->logger);
        $this->deferredHandler = new DeferredRedisTables(
            $this->metricStore->getNodeUuid(),
            $redisClient,
            new RedisTables($this->metricStore->getNodeUuid()->toString(), $this->mainRedis, $this->logger),
            $store,
            $this->logger
        );
        $this->deferredHandler->run();
    }

    protected function runSelfMonitoring(): void
    {
        $redis = $this->connectToRedis(ApplicationFeature::PROCESS_NAME . '::self-monitoring');
        $lua = new LuaScriptRunner($redis, dirname(__DIR__) . '/lua', $this->logger);

        $monitor = new SelfMonitoring(
            $redis,
            $this->connectToRrdCached(),
            $this->rrdtoolRunner,
            $this->logger,
            $this->metricStore->getUuid()->toString()
        );
        $monitor->watchProcessRunners([
            'redis-server' => $this->redisRunner,
            'rrdcached'    => $this->rrdCachedRunner,
            'metric-store' => $this,
        ]);
        $monitor->on(self::ON_MEASUREMENTS, function ($measurements) use ($lua) {
            // $this->logger->notice(print_r(array_map(JsonString::encode(...), $measurements), 1));
            $result = RedisResult::toArray(
                $lua->runScript(
                    'shipMeasurements',
                    array_map(JsonString::encode(...), $measurements)
                )
            );

            // TODO: remove, logging only
            $pairs = [];
            foreach ($result as $k => $v) {
                if ($v > 0) {
                    $pairs[] = "$k = $v";
                }
            }
            if (!empty($pairs)) {
                //$this->logger->notice('Shipped metrics: ' . implode(', ', $pairs));
            }
        });
        $monitor->run(15);
        $this->selfMonitoring = $monitor;
    }

    public function getProcessPid(): ?int
    {
        return getmypid();
    }

    protected static function getRrdToolBinary(): string
    {
        return static::DEFAULT_RRD_TOOL_BINARY;
    }
}
