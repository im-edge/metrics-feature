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

    public function claimMetricStore(MetricStore $store)
    {
        $store->setDataStoreUuid($this->getUuid());
    }

    protected function generateName() : string
    {
        if ($fqdn = gethostbyaddr(gethostbyname(gethostname()))) {
            return $fqdn;
        }

        throw new RuntimeException('Node name has not been set, FQDN detection failed');
    }
}
