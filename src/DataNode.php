<?php

namespace IcingaMetrics;

use RuntimeException;
use function gethostbyaddr;
use function gethostbyname;
use function gethostname;

class DataNode
{
    use DirectoryBasedComponent;

    const CONFIG_TYPE = 'IcingaMetrics/DataNode';
    const CONFIG_FILE_NAME = 'data-node.json';
    const CONFIG_VERSION = 'v1';
    const SUPPORTED_CONFIG_VERSIONS = [
        self::CONFIG_VERSION,
    ];

    /** @var MetricStore[] */
    protected array $metricStores = [];

    public function claimMetricStore(MetricStore $store)
    {
        $store->setDataStoreUuid($this->getUuid());
        $this->metricStores[$store->getUuid()->getBytes()] = $store;
    }

    public function getMetricStores(): array
    {
        return $this->metricStores;
    }

    protected function initialize()
    {
        $this->initializeRemoteApi();
        foreach ($this->config->getArray('registered-metric-stores') as $path) {
            $metrics = new MetricStore($path, $this->logger);
            $metrics->requireBeingConfigured();
            $this->claimMetricStore($metrics);
        }
    }

    protected function initializeRemoteApi()
    {
        $api = new DataNodeRemoteApi($this->logger, $this);
        $api->run('/run/icinga-metrics/' . $this->getUuid()->toString() . '.sock');
    }

    protected function generateName() : string
    {
        if ($fqdn = gethostbyaddr(gethostbyname(gethostname()))) {
            return $fqdn;
        }

        throw new RuntimeException('Node name has not been set, FQDN detection failed');
    }
}
