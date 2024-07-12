<?php

namespace IMEdge\MetricsFeature;

use Amp\Redis\RedisClient;
use gipfl\Json\JsonString;
use gipfl\RrdTool\AsyncRrdtool;
use gipfl\RrdTool\RrdCached\RrdCachedClient;
use IMEdge\MetricsFeature\Api\StoreApi\MinimalNodeApi;
use IMEdge\MetricsFeature\Api\StoreApi\RrdApi;
use IMEdge\MetricsFeature\FileInventory\DeferredRedisTables;
use IMEdge\MetricsFeature\Receiver\ReceiverRunner;
use IMEdge\MetricsFeature\RrdCached\RrdCachedRunner;
use IMEdge\Node\Rpc\ApiRunner;
use IMEdge\ProcessRunner\ProcessWithPidInterface;
use IMEdge\RedisRunner\RedisRunner;
use IMEdge\RedisTables\RedisTables;
use IMEdge\RedisUtils\LuaScriptRunner;
use IMEdge\RedisUtils\RedisResult;
use IMEdge\SimpleDaemon\DaemonComponent;
use Psr\Log\LoggerInterface;

use function Amp\async;
use function Amp\delay;
use function Amp\Future\awaitAll;
use function Amp\Redis\createRedisClient;
use function React\Async\await as awaitReact;

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
    protected RedisRunner $redisRunner;
    protected ?AsyncRrdtool $rrdtool = null;
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
        $this->rrdtool = new AsyncRrdtool(
            $this->rrdCachedRunner->getDataDirectory(),
            static::getRrdToolBinary(),
            $this->rrdCachedRunner->getSocketFile()
        );
        $this->rrdtool->setLogger($this->logger);
    }

    public function start(): void
    {
        $metricStore = $this->metricStore;
        $metricStore->run();
        $this->api->addApi(new MinimalNodeApi($this->metricStore, $this->api, $this->logger));
        $this->redisRunner->run();
        chdir($this->rrdCachedRunner->getDataDirectory());
        $this->rrdCachedRunner->run();
        delay(0.2);
        $this->runSelfMonitoring();
        $this->runDeferredHandler();
        $this->runMainHandler();
        $this->initializeRrdtool();
        delay(0.2);
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
            async(function () {
                awaitReact($this->rrdtool->endProcess());
            }),
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
        $rrdCached = new RrdCachedClient($this->rrdCachedRunner->getSocketFile());
        $this->api->addApi(new RrdApi($this->rrdtool, $rrdCached));
    }

    protected function runMainHandler(): void
    {
        $socket = $this->metricStore->getRedisSocketPath();
        $this->logger->info('MainHandler connecting to redis via ' . $socket);
        $rrdCached = new RrdCachedClient($this->rrdCachedRunner->getSocketFile());
        $this->mainHandler = new MainUpdateHandler($this->connectToRedis('main'), $rrdCached, $this->logger);
        $this->mainHandler->run();
    }

    protected function connectToRedis(string $clientNameSuffix): RedisClient
    {
        $client = createRedisClient('unix://' . $this->metricStore->getRedisSocketPath());
        $client->execute('CLIENT', 'SETNAME', ApplicationFeature::PROCESS_NAME . '::' . $clientNameSuffix);
        return $client;
    }

    protected function connectToRrdCached(): RrdCachedClient
    {
        $rrdCached = new RrdCachedClient($this->rrdCachedRunner->getSocketFile());
        $rrdCached->setLogger($this->logger);
        return $rrdCached;
    }

    protected function runDeferredHandler(): void
    {
        $rrdtool = new AsyncRrdtool(
            $this->rrdCachedRunner->getDataDirectory(),
            static::getRrdToolBinary()
        );
        $rrdtool->setLogger($this->logger);
        $this->deferredHandler = new DeferredRedisTables(
            $this->metricStore->getNodeUuid(),
            $this->connectToRedis('deferred'),
            new RedisTables($this->metricStore->getNodeUuid()->toString(), $this->mainRedis, $this->logger),
            $this->connectToRrdCached(),
            $rrdtool,
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
                $this->logger->notice('Shipped metrics: ' . implode(', ', $pairs));
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
