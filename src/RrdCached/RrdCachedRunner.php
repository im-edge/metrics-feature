<?php

namespace IcingaMetrics\RrdCached;

use gipfl\Stream\BufferedLineReader;
use IcingaMetrics\FilesystemUtil;
use IcingaMetrics\ProcessRunner;
use IcingaMetrics\ProcessWithPidInterface;
use Psr\Log\LoggerInterface;
use React\ChildProcess\Process;
use React\EventLoop\Loop;

class RrdCachedRunner implements ProcessWithPidInterface
{
    protected $socketFile;

    /** @var ProcessRunner */
    protected $runner;

    /** @var string */
    protected $baseDir;

    /** @var string */
    protected $binary;

    /** @var LoggerInterface */
    protected $logger;

    public function __construct(string $binary, string $baseDir, LoggerInterface $logger)
    {
        if (! is_executable($binary)) {
            throw new \RuntimeException("Cannot execute $binary");
        }
        $this->binary = $binary;
        $this->baseDir = $baseDir;
        $this->logger = $logger;
    }

    public function getSocketFile()
    {
        return $this->socketFile;
    }

    public function run()
    {
        $dir = $this->baseDir;
        FilesystemUtil::requireDirectory($dir);
        $this->runner = new ProcessRunner($this->binary, $this->prepareArguments());
        $this->runner->on(ProcessRunner::ON_START, function (Process $process) {
            $lines = new BufferedLineReader("\n", Loop::get());
            $lines->on('line', function ($data) {
                $this->logger->info($data);
            });
            $process->stdout->pipe($lines);
            $errorLines = new BufferedLineReader("\n", Loop::get());
            $errorLines->on('line', function ($data) {
                $this->logger->info('ERR: ' . $data);
            });
            $process->stdout->pipe($lines);
        });
        $this->runner->setLogger($this->logger)->run();
    }

    public function getDataDir(): string
    {
        return $this->baseDir . '/data';
    }

    protected function prepareArguments(): array
    {
        $path = $this->baseDir;
        $baseDir = "$path/data";
        $journalDir = "$path/journal";
        FilesystemUtil::requireDirectory($baseDir);
        FilesystemUtil::requireDirectory($journalDir);
        $this->socketFile = $sockFile = "$path/rrdcached.sock";
        $pidFile = "$path/rrdcached.pid";
        if (file_exists($pidFile)) {
            $this->logger->notice("Unlinking PID file in $pidFile");
            unlink($pidFile);
        }
        $sockMode = '0660';
        // $sockGroup = 'icingaweb2';
        return [
            '-B', // Only permit writes into the base directory specified
            // '-L', // NETWORK_OPTIONS
            '-b', $baseDir,
            '-R', // Permit recursive subdirectory creation in the base directory (only with -B)
            // NOT setting -F, we want a fast shutdown, not flushing as there is a journal
            '-j', $journalDir,
            '-p', $pidFile,
            // Order matters, -m and -s affect FOLLOWING sockets
            // -s  Unix socket group permissions: numeric group id or group name
            '-m', $sockMode,
            // '-s $sockGroup',
            '-l', "unix:$sockFile",
            '-g', // Run in foreground
            // '-U', $daemonUser,
            // '-G', $daemonGroup,
            // -P FLUSH -> restrict permissions
            // -O Do not allow CREATE commands to overwrite existing files, even if asked to.
            '-w', '3600', // write timeout, data is written to disk every timeout seconds. 3600?
            // '-z', '0', //  Write Jitter, spread load - only when > 0
            '-z', '1800',
            '-t', '4', // Write Threads
            '-f', '28800', // search cache for old values that have been written to disk - this
            // removes files which get no more updates from cache
            // Only 1.7:
            // '-V', 'LOG_INFO', // LOG_EMERG, LOG_ALERT, LOG_CRIT, LOG_ERR, LOG_WARNING, LOG_NOTICE,
            //       // LOG_INFO, LOG_DEBUG. Default is LOG_ERR
        ];
    }

    public function getProcessPid(): ?int
    {
        return $this->runner->getChildPid();
    }
}
