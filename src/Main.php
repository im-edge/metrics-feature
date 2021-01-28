<?php

namespace iPerGraph;

use Exception;
use gipfl\Log\Filter\LogLevelFilter;
use gipfl\Log\Logger;
use gipfl\Log\Writer\SystemdStdoutWriter;
use Psr\Log\LoggerInterface;
use React\EventLoop\Factory as Loop;
use React\EventLoop\LoopInterface;

class Main
{
    /** @var LoopInterface */
    protected $loop;

    /** @var LoggerInterface */
    protected $logger;

    public function __construct()
    {
        $this->loop = Loop::create();
        $this->initializeLogger();
        $this->registerSignalHandlers();
    }

    public function startLoop()
    {
        $this->loop->run();
    }

    public function getLoop()
    {
        return $this->loop;
    }

    public function getLogger()
    {
        return $this->logger;
    }

    protected function registerSignalHandlers()
    {
        $this->loop->addSignal(SIGINT, $func = function ($signal) use (&$func) {
            $this->shutdownWithSignal($signal, $func);
        });
        $this->loop->addSignal(SIGTERM, $func = function ($signal) use (&$func) {
            $this->shutdownWithSignal($signal, $func);
        });
    }

    protected function shutdownWithSignal($signal, &$func)
    {
        $this->loop->removeSignal($signal, $func);
        $this->shutdown($signal);
    }

    protected function shutdown($signal = null)
    {
        // $this->isReady = false;

        try {
            $this->logSignalInfo($signal);
            // $this->redis->end();
            // $this->rrdCached->quit();
        } catch (Exception $e) {
            $this->logShutdownFailure($e);
        }

        $this->loop->futureTick(function () {
            $this->loop->stop();
        });
    }


    protected function logSignalInfo($signal)
    {
        if ($signal === null) {
            $this->logger->info('Shutting down');
        } else {
            $this->logger->info(sprintf(
                'Got %s signal, shutting down',
                $this->getSignalName($signal)
            ));
        }
    }

    protected function logShutdownFailure(Exception $e)
    {
        $this->logger->error(sprintf(
            'Failed to safely shutdown: %s -> %s, stopping anyways',
            $e->getMessage(),
            $e->getTraceAsString()
        ));
    }

    /**
     * @param int $signal
     * @return string
     */
    protected function getSignalName($signal)
    {
        switch ($signal) {
            case SIGINT:
                return '<INT>';
            case SIGTERM:
                return '<TERM>';
            default:
                return '<UNKNOWN>';
        }
    }

    protected function initializeLogger()
    {
        $logger = new Logger();
        $logger->addWriter(new SystemdStdoutWriter($this->loop));
        // $logger->addFilter(new LogLevelFilter('notice'));
        $this->logger = $logger;
    }
}
