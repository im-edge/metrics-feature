<?php

namespace IcingaMetrics;

use gipfl\SimpleDaemon\DaemonTask;
use gipfl\SimpleDaemon\SystemdAwareTask;
use gipfl\SystemD\NotifySystemD;
use IcingaMetrics\Db\ZfDbConnectionFactory;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use function React\Promise\resolve;

class DataNodeRunner implements DaemonTask, LoggerAwareInterface, SystemdAwareTask
{
    use LoggerAwareTrait;

    /** @var NotifySystemD|boolean */
    protected $systemd;
    protected DataNode $dataNode;

    public function __construct(DataNode $dataNode)
    {
        $this->dataNode = $dataNode;
    }

    public function start(LoopInterface $loop)
    {
        $this->initialize();
        return resolve();
    }

    protected function initialize()
    {
        $logger = $this->logger;
        $dataNode = $this->dataNode;
        $dataNode->run();
        $db = ZfDbConnectionFactory::connection(
            (array) $dataNode->requireConfig()->getAsSettings('db')->jsonSerialize()
        );
        $dbInventory = new DbInventory($db, $logger);
        $dbInventory->registerDataNode($dataNode);

        foreach ($dataNode->getMetricStores() as $metricStore) {
            $dbInventory->registerMetricsStore($metricStore);
            $metricStore->run();
            Loop::futureTick(function () use ($dbInventory, $metricStore, $logger) {
                $redis = new RedisPerfDataApi($logger, $metricStore->getRedisSocketUri());
                $redis->setClientName(Application::PROCESS_NAME . '::ciUpdates');
                $handler = new CiUpdateHandler($dbInventory, $redis, $metricStore->getUuid(), $logger);
                $handler->run();
            });
        }
    }

    public function stop()
    {
        return resolve();
    }

    public function setSystemd(NotifySystemD $systemd)
    {
        $this->systemd = $systemd;
    }
}
