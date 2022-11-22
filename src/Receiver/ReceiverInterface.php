<?php

namespace IcingaMetrics\Receiver;

use gipfl\DataType\Settings;
use IcingaMetrics\MetricStore;
use Psr\Log\LoggerInterface;

interface ReceiverInterface
{
    public function __construct(LoggerInterface $logger, Settings $settings, MetricStore $metricStore);

    public function run();
}
