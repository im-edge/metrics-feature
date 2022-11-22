<?php

namespace IcingaMetrics;

use gipfl\Protocol\JsonRpc\Handler\NamespacedPacketHandler;
use gipfl\Protocol\JsonRpc\JsonRpcConnection;
use Psr\Log\LoggerInterface;

class DataNodeRemoteApi extends BaseRemoteApi
{
    protected DataNode $dataNode;

    public function __construct(LoggerInterface $logger, DataNode $dataNode)
    {
        $this->dataNode = $dataNode;
        parent::__construct($logger);
    }

    protected function addHandlersToJsonRpcConnection(JsonRpcConnection $connection)
    {
        $handler = new NamespacedPacketHandler();
        $dataHandler = new RpcNamespaceDatanode($this->dataNode);
        $handler->registerNamespace('rrd', $dataHandler);
        $connection->setHandler($handler);
    }
}
