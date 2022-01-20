<?php

namespace IcingaMetrics;

use Evenement\EventEmitterInterface;
use Evenement\EventEmitterTrait;
use gipfl\DataType\Settings;
use gipfl\IcingaApi\ApiEvent\CheckResultApiEvent;
use gipfl\IcingaApi\IcingaStreamingClient;
use gipfl\IcingaApi\ReactGlue\PeerCertificate;
use gipfl\IcingaPerfData\Ci;
use Psr\Log\LoggerInterface;
use React\EventLoop\Loop;
use React\Socket\ConnectionInterface;

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
                    echo $pem;
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
            foreach ($points as $point) {
                $values[$point->getLabel()] = $point->getValue();
            }
            // TODO: check_command as context?
            $ci = new Ci($result->getHost(), $result->isHost() ? null : $result->getService());
            $this->emit('perfData', [new PerfData($ci, $values, $checkResult->getExecutionEnd())]);
        });
        Loop::futureTick(function () {
            $this->logger->info('Initializing Icinga Streaming Client');
            $this->streamer->run();
        });
    }
}
