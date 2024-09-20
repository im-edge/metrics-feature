<?php

namespace IMEdge\MetricsFeature\Receiver;

use IMEdge\Config\Settings;
use IMEdge\MetricsFeature\MetricStore;
use Psr\Log\LoggerInterface;

interface ReceiverInterface
{
    public function __construct(LoggerInterface $logger, Settings $settings, MetricStore $metricStore);

    public function run(): void;
}
