<?php

namespace IcingaMetrics;

use Evenement\EventEmitterInterface;
use Evenement\EventEmitterTrait;
use gipfl\IcingaApi\ApiEvent\CheckResultApiEvent;
use gipfl\IcingaApi\IcingaStreamingClient;
use gipfl\IcingaApi\ReactGlue\PeerCertificate;
use Psr\Log\LoggerInterface;
use React\EventLoop\LoopInterface;
use React\Socket\ConnectionInterface;

class IcingaStreamer implements EventEmitterInterface
{
    use EventEmitterTrait;

    /** @var LoopInterface */
    protected $loop;

    /** @var LoggerInterface */
    protected $logger;

    /** @var IcingaStreamingClient */
    protected $streamer;

    public function __construct(LoopInterface $loop, LoggerInterface $logger, $icingaConfig)
    {
        $this->loop = $loop;
        $this->logger = $logger;
        $this->streamer = $streamer = new IcingaStreamingClient(
            $loop,
            $icingaConfig->host,
            $icingaConfig->port,
            $icingaConfig->user,
            $icingaConfig->pass
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
            $ciName = $result->getHost() . '!' . (
                $result->isHost()
                    ? '_HOST_'
                    : $result->getService()
                );
            $this->logger->info($ciName);
            $checkResult = $result->getCheckResult();
            // $this->logger->info(print_r($checkResult->getCommand()));
            $points = $checkResult->getDataPoints();
            if (count($points) === 0) {
                // $logger->info("Skipping $ciName, no perfdata");
                return;
            }

            $values = (object) [];
            foreach ($points as $point) {
                $values->{$point->getLabel()} = $point->getValue();
            }
            // TODO: check_command as context?
            $this->emit('perfData', [new PerfData($ciName, $values, $checkResult->getExecutionEnd())]);
        });
        $this->loop->futureTick(function () {
            $this->logger->info('Initializing Icinga Streaming Client');
            $this->streamer->run();
        });
    }
}
