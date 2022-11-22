<?php

namespace IcingaMetrics;

use React\Promise\ExtendedPromiseInterface;
use stdClass;

class RpcNamespaceDatanode
{
    protected DataNode $node;

    public function __construct(DataNode $node)
    {
        $this->node = $node;
    }

    public function getNameRequest(): string
    {
        return $this->node->getName();
    }

    public function getUUidRequest(): string
    {
        return $this->node->getUuid()->toString();
    }
    public function createStoreRequest(string $name): ExtendedPromiseInterface
    {
    }

    public function getStoresRequest(): stdClass
    {
        $result = [];
        foreach ($this->node->getMetricStores() as $store) {
            $result[$store->getUuid()->toString()] = (object) [
                'name'    => $store->getName(),
                'directory' => $store->getBaseDir(),
            ];
        }

        return (object) $result;
    }
}
