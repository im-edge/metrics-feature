<?php

namespace IMEdge\MetricsFeature;

use Evenement\EventEmitterInterface;
use Evenement\EventEmitterTrait;
use gipfl\DataType\Settings;
use gipfl\IcingaApi\ApiEvent\CheckResultApiEvent;
use gipfl\IcingaApi\DataPoint;
use gipfl\IcingaApi\IcingaStreamingClient;
use IMEdge\Metrics\Ci;
use IMEdge\Metrics\Measurement;
use IMEdge\Metrics\Metric;
use IMEdge\Metrics\MetricDatatype;
use Psr\Log\LoggerInterface;

class IcingaStreamer implements EventEmitterInterface
{
    use EventEmitterTrait;

    protected IcingaStreamingClient $streamer;
    protected LoggerInterface $logger;

    public function __construct(LoggerInterface $logger, Settings $icingaConfig)
    {
        $this->logger = $logger;
        $this->streamer = $streamer = new IcingaStreamingClient(
            Loop::get(),
            $icingaConfig->get('host'),
            (int) $icingaConfig->get('port', 5665),
            $icingaConfig->get('user'),
            $icingaConfig->get('pass')
        );
        $this->streamer->setLogger($logger);
        $streamer->filterTypes(['CheckResult']);
        $streamer->on('connection', function (ConnectionInterface $connection) {
            $peer = PeerCertificate::eventuallyGetTlsPeer($connection);
            if ($peer && $cert = $peer->getPeerCertificate()) {
                if (openssl_x509_export($cert, $pem)) {
                    // echo $pem;
                }
            }
        });
        $streamer->on('checkResult', function (CheckResultApiEvent $result) {
            $checkResult = $result->getCheckResult();
            // $this->logger->info(print_r($checkResult->getCommand()));
            $points = $checkResult->getDataPoints();
            if (count($points) === 0) {
                // $logger->info("Skipping $ciName, no perfdata");
                return;
            }

            $values = [];
            $metrics = [];
            /** @var DataPoint $point */
            foreach ($points as $point) {
                $values[$point->getLabel()] = $point->getValue();
                $dataType = $point->getUnit() === 'c' ? MetricDatatype::COUNTER : MetricDatatype::GAUGE;
                $metrics[] = new Metric($point->getLabel(), $point->getValue(), $dataType, $point->getUnit());
            }
            // TODO: check_command as context?
            $ci = new Ci($result->getHost(), $result->isHost() ? null : $result->getService());
            $this->emit(MetricStoreRunner::ON_MEASUREMENTS, [[new Measurement(
                $ci,
                (int) floor($checkResult->getExecutionEnd()),
                $metrics
            )]]);
        });
        Loop::futureTick(function () {
            $this->logger->info('Initializing Icinga Streaming Client');
            $this->streamer->run();
        });
    }
}
