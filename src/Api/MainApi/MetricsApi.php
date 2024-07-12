<?php

namespace IMEdge\MetricsFeature\Api\MainApi;

use gipfl\DataType\Settings;
use IMEdge\MetricsFeature\FeatureRunner;
use IMEdge\MetricsFeature\MetricStore;
use IMEdge\RpcApi\ApiMethod;
use IMEdge\RpcApi\ApiNamespace;
use InvalidArgumentException;
use Psr\Log\LoggerInterface;
use Ramsey\Uuid\UuidInterface;
use stdClass;

#[ApiNamespace('metrics')]
class MetricsApi
{
    public function __construct(
        protected FeatureRunner $runner,
        protected readonly LoggerInterface $logger
    ) {
    }

    /**
     * Create a new Metrics Store
     *
     * This will create a Metric Store directory structure in the given directory,
     * configure and start a dedicated daemon with related helper daemons (Redis,
     * RRDcached) and begin accepting Metrics and shipping Graphs
     */
    #[ApiMethod]
    public function createStore(string $name, string $baseDir): Settings
    {
        $store = new MetricStore($baseDir, $this->logger);
        $store->setName($name);
        $this->runner->claimMetricStore($store);
        return $store->requireConfig();
    }

    #[ApiMethod]
    public function deleteStore(UuidInterface $uuid): bool
    {
        throw new \Exception('Not yet');
    }

    #[ApiMethod]
    public function getStores(): stdClass
    {
        $result = [];
        foreach ($this->runner->getMetricStores() as $store) {
            $uuid = $store->getUuid()->toString();
            $result[$uuid] = (object) [
                'name' => $store->getName(),
                'uuid' => $uuid,
                'path' => $store->getBaseDir(),
            ];
        }

        return (object) $result;
    }

    #[ApiMethod]
    public function getMetricStoreSettings(UuidInterface $uuid): Settings
    {
        foreach ($this->runner->getMetricStores() as $store) {
            if ($store->getUuid()->equals($uuid)) {
                return $store->requireConfig();
            }
        }

        throw new InvalidArgumentException('Found no MetricStore with UUID=' . $uuid->toString());
    }
}
