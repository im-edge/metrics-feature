<?php

namespace IcingaMetrics\Receiver;

use gipfl\DataType\Settings;
use IcingaMetrics\MetricStore;
use Psr\Log\LoggerInterface;

abstract class BaseReceiver implements ReceiverInterface
{
    protected LoggerInterface $logger;
    protected Settings $settings;
    protected MetricStore $metricStore;

    public function __construct(LoggerInterface $logger, Settings $settings, MetricStore $metricStore)
    {
        $this->logger = $logger;
        $this->metricStore = $metricStore;
        $this->settings = $settings;
    }

    abstract public function run();
}
