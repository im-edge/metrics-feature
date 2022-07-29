<?php declare(strict_types=1);

namespace IcingaMetrics;

use Evenement\EventEmitter;
use gipfl\Process\FinishedProcessState;
use gipfl\Process\ProcessKiller;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use React\ChildProcess\Process;
use React\EventLoop\Loop;
use React\EventLoop\TimerInterface;
use React\Promise\Deferred;
use React\Promise\ExtendedPromiseInterface;
use function array_map;
use function implode;

class ProcessRunner extends EventEmitter
{
    const ON_START = 'start';

    /** @var  Process */
    protected $process;

    /** @var string */
    protected $command;

    /** @var array */
    protected $args = [];

    /** @var ?array */
    protected $env;

    /** @var ?string */
    protected $cwd;

    /** @var ?Deferred */
    protected $terminating;

    /** @var ?TimerInterface */
    protected $scheduledRestart;

    /** @var int */
    protected $childPid;

    /** @var LoggerInterface */
    protected $logger;

    /** @var bool */
    protected $restartOnSuccess = false;

    public function __construct(string $command, array $args = null, string $cwd = null, array $env = null)
    {
        $this->setLogger(new NullLogger());
        $this->command = $command;
        if ($args !== null) {
            $this->args = $args;
        }
        $this->cwd = $cwd;
        $this->env = $env;
    }

    /**
     * @param LoggerInterface $logger
     * @return $this
     */
    public function setLogger(LoggerInterface $logger): ProcessRunner
    {
        $this->logger = $logger;
        return $this;
    }

    public function terminate(int $timeout = 2): ExtendedPromiseInterface
    {
        if ($this->terminating) {
            return $this->terminating->promise();
        }

        $this->terminating = new Deferred();
        if ($this->process) {
            ProcessKiller::terminateProcess($this->process, Loop::get(), $timeout)->then(function () {
                $this->process = null;
                $this->terminating->resolve();
            });
        } else {
            $this->logger->notice("Tried to terminate process, but it's already gone");
            Loop::get()->futureTick(function () {
                if ($this->terminating) {
                    $this->terminating->resolve();
                }
            });
        }

        return $this->terminating->promise();
    }

    /**
     * @param bool $restart
     * @return $this
     */
    public function restartOnSuccess(bool $restart): ProcessRunner
    {
        $this->restartOnSuccess = $restart;
        return $this;
    }

    protected function runAgain(int $delay = 0)
    {
        if ($this->terminating) {
            throw new \RuntimeException('Cannot run again while terminating');
        }
        if ($this->scheduledRestart) {
            return;
        }
        $this->scheduledRestart = Loop::get()->addTimer($delay, function () {
            $this->scheduledRestart = null;
            try {
                $this->run();
            } catch (\Throwable $e) {
                $this->logger->error($e->getMessage());
                $this->scheduleRestart($e->getMessage(), 10);
            }
        });
    }

    protected function scheduleRestart(string $reason, int $delay)
    {
        $this->logger->error(sprintf(
            'Process failed: %s, restarting in %d seconds',
            $reason,
            $delay
        ));
        $this->runAgain($delay);
    }

    public function run()
    {
        $cmd = 'exec ' . $this->command . $this->getArgumentString();
        $this->logger->info("Running $cmd");
        $process = new Process($cmd, $this->cwd, $this->env);
        $process->on('error', function (\Exception $e) {
            $this->scheduleRestart($e->getMessage(), 5);
        });

        $process->on('exit', function ($exitCode, $termSignal) {
            $this->process = null;
            $state = new FinishedProcessState($exitCode, $termSignal);
            if ($state->succeeded()) {
                $this->logger->info(sprintf('%s(%d) finished', $this->command, $this->childPid));
            } elseif (! $this->terminating) {
                $this->scheduleRestart($state->getReason(), 5);
            }
        });

        $process->start(Loop::get());
        $this->childPid = $process->getPid();
        $this->process = $process;
        $this->emit(self::ON_START, [$process]);
    }

    public function getChildPid(): ?int
    {
        return $this->childPid;
    }

    protected function getArgumentString(): string
    {
        if (empty($this->args)) {
            return '';
        }

        return ' ' . implode(' ', array_map('escapeshellarg', $this->args));
    }
}
