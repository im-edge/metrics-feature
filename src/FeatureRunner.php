<?php

namespace IMEdge\MetricsFeature;

use gipfl\Json\JsonString;
use gipfl\Log\PrefixLogger;
use IMEdge\Metrics\Measurement;
use IMEdge\MetricsFeature\Api\PerStore\LogApi;
use IMEdge\MetricsFeature\Store\StoreCommandRunner;
use IMEdge\Node\Feature;
use IMEdge\Node\Rpc\ApiRunner;
use IMEdge\Node\Rpc\Routing\Node;
use IMEdge\RedisUtils\LuaScriptRunner;
use IMEdge\RedisUtils\RedisResult;
use Psr\Log\LoggerInterface;
use Revolt\EventLoop;

use function Amp\Redis\createRedisClient;

class FeatureRunner
{
    protected string $binary;
    /** @var MetricStore[] */
    protected array $metricStores = [];
    /** @var LuaScriptRunner[] */
    protected array $redisClients = [];
    /** @var StoreCommandRunner[] */
    protected array $processRunners = [];

    public function __construct(
        protected readonly Feature $feature,
        protected readonly LoggerInterface $logger
    ) {
        $this->binary = $this->feature->getBinaryFile('imedge-metricstore');
    }

    public function run(): void
    {
        $this->initializeMetricStores();
    }

    public function stop(): void
    {
        $runners = $this->processRunners;
        foreach ($runners as $runner) {
            $runner?->stop();
        }
    }

    /**
     * @param Measurement[] $measurements
     */
    public function shipMeasurements(array $measurements, string $storeName): void
    {
        $redis = $this->redisClients[$storeName] ?? throw new \RuntimeException("There is no $storeName Metrics Store");
        $result = RedisResult::toHash( // TODO: Either emit as related metrics, log errors - or to not even convert
            $redis->runScript('shipMeasurements', array_map(JsonString::encode(...), $measurements))
        );
    }

    protected function initializeMetricStores(): void
    {
        foreach ($this->feature->settings->getArray('registered-metric-stores') as $path) {
            EventLoop::queue(fn () => $this->initializeMetricStore($path));
        }
    }

    protected function initializeMetricStore(string $path): void
    {
        try {
            $metrics = new MetricStore($path, $this->logger);
            $metrics->requireBeingConfigured();
            $this->claimMetricStore($metrics);
            $this->startMetricStore($metrics);
        } catch (\Throwable $e) {
            $this->logger->error('MetricStore initialization failed: ' . $e->getMessage());
        }
    }

    protected function startMetricStore(MetricStore $metricStore): void
    {
        $name = $metricStore->getName();
        $api = new ApiRunner($metricStore->getUuid()->toString());
        $api->addApi(new LogApi($this->logger, "[$name (child)] "));
        $runner = new StoreCommandRunner(
            $this->binary,
            $metricStore->getBaseDir(),
            new PrefixLogger("[$name (parent)] ", $this->logger)
        );
        $runner->setArguments([
            $metricStore->getBaseDir(),
            $this->feature->services->getRedisSocket(),
            '--debug'
        ]);
        $runner->setHandler($api);
        $runner->run();
        $this->processRunners[$name] = $runner;
        $socket = 'unix://' . $metricStore->getRedisSocketPath();
        $rpc = $runner->jsonRpc;
        $this->logger->info("Metrics feature connecting to redis ($name) via " . $socket);
        $redis = createRedisClient($socket);
        $this->redisClients[$name] = new LuaScriptRunner($redis, dirname(__DIR__) . '/lua', $this->logger);
        $myPeerAddress = 'process:///' . $runner->getProcessPid();
        $this->feature->connectNode(new Node($metricStore->getUuid(), $myPeerAddress, $rpc));
    }

    public function getMetricStores(): array
    {
        return $this->metricStores;
    }

    public function claimMetricStore(MetricStore $store): void
    {
        $store->setNodeUuid($this->feature->nodeIdentifier->uuid);
        $this->metricStores[$store->getUuid()->getBytes()] = $store;
        $path = $store->getBaseDir();
        $registered = $this->feature->settings->getArray('registered-metric-stores');
        if (! in_array($path, $registered)) {
            $registered[] = $path;
            $this->feature->settings->set('registered-metric-stores', $registered);
            $this->feature->storeSettings();
        }
    }
}
