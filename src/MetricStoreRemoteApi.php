<?php

namespace IcingaMetrics;

use gipfl\Protocol\JsonRpc\Handler\NamespacedPacketHandler;
use gipfl\Protocol\JsonRpc\JsonRpcConnection;
use gipfl\RrdTool\AsyncRrdtool;
use gipfl\RrdTool\RrdCached\Client as RrdCachedClient;
use Psr\Log\LoggerInterface;

class MetricStoreRemoteApi extends BaseRemoteApi
{
    protected AsyncRrdtool $rrdtool;
    protected RrdCachedClient $rrdCached;

    public function __construct(
        LoggerInterface $logger,
        AsyncRrdtool $rrdtool,
        RrdCachedClient $rrdCached
    ) {
        $this->rrdtool = $rrdtool;
        $this->rrdCached = $rrdCached;
        parent::__construct($logger);
    }

    protected function addHandlersToJsonRpcConnection(JsonRpcConnection $connection)
    {
        $handler = new NamespacedPacketHandler();
        $rrdHandler = new RpcNamespaceRrd($this->rrdtool, $this->rrdCached);
        $handler->registerNamespace('rrd', $rrdHandler);
        $connection->setHandler($handler);
    }
}
