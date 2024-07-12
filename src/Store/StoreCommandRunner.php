<?php

namespace IMEdge\MetricsFeature\Store;

use Amp\Process\Process;
use IMEdge\JsonRpc\JsonRpcConnection;
use IMEdge\JsonRpc\RequestHandler;
use IMEdge\Log\LogHelper;
use IMEdge\ProcessRunner\BufferedLineReader;
use IMEdge\ProcessRunner\ProcessRunnerHelper;
use IMEdge\Protocol\NetString\NetStringConnection;

use function Amp\ByteStream\pipe;

class StoreCommandRunner extends ProcessRunnerHelper
{
    public ?JsonRpcConnection $jsonRpc = null;
    protected array $arguments = [];
    protected ?RequestHandler $handler = null;

    protected function initialize(): void
    {
    }

    public function setHandler(RequestHandler $handler): void
    {
        $this->handler = $handler;
    }

    public function setArguments(array $arguments): void
    {
        $this->arguments = $arguments;
    }

    protected function onStartingProcess(): void
    {
        $this->logger->notice('Starting store process');
    }

    protected function onProcessStarted(Process $process): void
    {
        $netString = new NetStringConnection($this->process->getStdout(), $this->process->getStdin());
        $this->jsonRpc = new JsonRpcConnection($netString, $netString, $this->handler, $this->logger);
        $stdErrReader = new BufferedLineReader(static function (string $line) {
            $this->logger->error($line);
        }, "\n");
        pipe($process->getStderr(), $stdErrReader);
    }

    protected function getArguments(): array
    {
        return $this->arguments;
    }
}
