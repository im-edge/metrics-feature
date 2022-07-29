<?php

namespace IcingaMetrics\Redis;

use gipfl\Stream\BufferedLineReader;
use IcingaMetrics\FilesystemUtil;
use IcingaMetrics\ProcessRunner;
use IcingaMetrics\ProcessWithPidInterface;
use Psr\Log\LoggerInterface;
use React\ChildProcess\Process;
use React\EventLoop\Loop;

class RedisRunner implements ProcessWithPidInterface
{
    protected ProcessRunner $runner;
    protected string $baseDir;
    protected string $binary;
    protected LoggerInterface $logger;
    protected ?int $redisPid;

    public function __construct(string $binary, string $baseDir, LoggerInterface $logger)
    {
        if (! is_executable($binary)) {
            throw new \RuntimeException("Cannot execute $binary");
        }
        $this->binary = $binary;
        $this->baseDir = $baseDir;
        $this->logger = $logger;
    }

    public function getSocketUri(): string
    {
        return 'redis+unix://' . $this->baseDir . '/redis.sock';
    }

    public function getProcessPid(): ?int
    {
        return $this->redisPid;
    }

    public function run()
    {
        $dir = $this->baseDir;
        $redisConf = "$dir/redis.conf";
        FilesystemUtil::requireDirectory($dir);
        file_put_contents($redisConf, RedisConfigGenerator::forPath($dir));
        $this->runner = new ProcessRunner($this->binary, [$redisConf]);
        $this->runner->on(ProcessRunner::ON_START, function (Process $process) {
            $this->redisPid = $process->getPid();
            $lines = new BufferedLineReader("\n", Loop::get());
            $lines->on('line', function ($data) {
                $this->logger->info($data);
            });
            $process->stdout->pipe($lines);
        });
        $this->runner->setLogger($this->logger)->run();
    }
}
