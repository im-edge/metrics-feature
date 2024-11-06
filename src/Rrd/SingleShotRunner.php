<?php

namespace IMEdge\MetricsFeature\Rrd;

use Amp\ByteStream\Payload;
use Amp\Process\Process;
use Amp\Process\ProcessException;
use IMEdge\ProcessRunner\BufferedLineReader;
use Psr\Log\LoggerInterface;

use function Amp\async;

class SingleShotRunner
{
    public function __construct(
        protected readonly LoggerInterface $logger
    ) {
    }

    /**
     * Run the given command, return its exit code
     *
     * @return array{0: int, 1: string}
     * @throws ProcessException
     */
    public function run(string $command, ?string $workingDirectory = null): array
    {
        $process = Process::start($command, $workingDirectory);
        async(function () use (&$stdout, $process) {
            $stdout = (new Payload($process->getStdout()))->buffer();
        });
        $stdErrReader = new BufferedLineReader($this->logger->error(...), "\n");

        return [$process->join(), $stdout];
    }
}
