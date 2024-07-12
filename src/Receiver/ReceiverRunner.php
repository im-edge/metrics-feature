<?php

namespace IMEdge\MetricsFeature\Receiver;

use gipfl\DataType\Settings;
use IMEdge\MetricsFeature\MetricStore;
use Psr\Log\LoggerInterface;
use stdClass;

class ReceiverRunner
{
    protected LoggerInterface $logger;
    protected Settings $settings;
    protected MetricStore $metricStore;
    protected stdClass $receivers;

    public function __construct(LoggerInterface $logger, stdClass $receivers, MetricStore $metricStore)
    {
        $this->logger = $logger;
        $this->receivers = $receivers;
        $this->metricStore = $metricStore;
    }

    public function run(): void
    {
        foreach ($this->receivers as $uuid => $config) {
            $config = Settings::fromSerialization($config);
            $logName = $config->get('name', $uuid);
            if ($config->get('enabled')) {
                $implementation = $config->get('implementation');
                if ($implementation) {
                    $className = __NAMESPACE__ . "\\{$implementation}Receiver";
                    if (class_exists($className)) {
                        $receiver = new $className(
                            $this->logger,
                            $config->getAsSettings('settings'),
                            $this->metricStore
                        );
                        if ($receiver instanceof ReceiverInterface) {
                            $this->logger->notice("Starting receiver: $logName");
                            $receiver->run();
                        } else {
                            $this->logger->error("'$className' ('$logName') is not a ReceiverInterface implementation");
                        }
                    } else {
                        $this->logger->error("ReceiverInterface implementation '$className' ('$logName') not found");
                    }
                } else {
                    $this->logger->error("Receiver '$logName' has no implementation");
                }
            } else {
                $this->logger->notice("Receiver '$logName' is not enabled");
            }
        }
    }
}
