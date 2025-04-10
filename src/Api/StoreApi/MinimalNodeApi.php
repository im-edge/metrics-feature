<?php

namespace IMEdge\MetricsFeature\Api\StoreApi;

use IMEdge\Config\Settings;
use IMEdge\Inventory\NodeIdentifier;
use IMEdge\MetricsFeature\MetricStore;
use IMEdge\Node\Rpc\ApiRunner;
use IMEdge\RpcApi\ApiMethod;
use IMEdge\RpcApi\ApiNamespace;
use Psr\Log\LoggerInterface;
use Ramsey\Uuid\UuidInterface;

#[ApiNamespace('node')]
class MinimalNodeApi
{
    protected NodeIdentifier $identifier;

    public function __construct(
        protected MetricStore $store,
        protected ApiRunner $api,
        protected LoggerInterface $logger
    ) {
        $this->identifier = new NodeIdentifier(
            $this->store->getUuid(),
            $this->store->getNodeUuid()->toString() . '/' . $this->store->getName(),
            gethostbyaddr(gethostbyname(gethostname()))
        );
    }

    #[ApiMethod]
    public function getIdentifier(): NodeIdentifier
    {
        return $this->identifier;
    }

    #[ApiMethod]
    public function getSettings(): Settings
    {
        return $this->store->requireConfig();
    }

    #[ApiMethod]
    public function getName(): string
    {
        return $this->identifier->name;
    }

    #[ApiMethod]
    public function getUuid(): UuidInterface
    {
        return $this->identifier->uuid;
    }

    #[ApiMethod]
    public function getAvailableMethods(): array
    {
        try {
            return $this->api->getKnownMethods();
        } catch (\Throwable $e) {
            return [$e->getMessage()];
        }
    }

    #[ApiMethod]
    public function getConnections(): array
    {
        return [];
    }

    #[ApiMethod]
    public function getFeatures(): object
    {
        return (object)[];
    }

    #[ApiMethod]
    public function listListeners(): array
    {
        return [];
    }
}
