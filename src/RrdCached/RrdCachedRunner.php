<?php

namespace IMEdge\MetricsFeature\RrdCached;

use Amp\Process\Process;
use IMEdge\Filesystem\Directory;
use IMEdge\ProcessRunner\BufferedLineReader;
use IMEdge\ProcessRunner\ProcessRunnerHelper;

use function Amp\async;
use function Amp\ByteStream\pipe;

class RrdCachedRunner extends ProcessRunnerHelper
{
    protected string $applicationName = 'rrdcached';

    public function getSocketFile(): string
    {
        return $this->baseDir . '/rrdcached.sock';
    }

    public function getDataDirectory(): string
    {
        return $this->baseDir . '/data';
    }

    protected function getJournalDirectory(): string
    {
        return $this->baseDir . '/journal';
    }

    protected function getPidFile(): string
    {
        return $this->baseDir . '/rrdcached.pid';
    }

    protected function onStartingProcess(): void
    {
        Directory::requireWritable($this->baseDir);
        Directory::requireWritable($this->getJournalDirectory());
        Directory::requireWritable($this->getDataDirectory());
        $pidFile = $this->getPidFile();
        if (file_exists($pidFile)) {
            $this->logger->notice("Unlinking PID file in $pidFile");
            unlink($pidFile);
        }
    }

    protected function onProcessStarted(Process $process): void
    {
        async(function () use ($process) {
            $stdOutReader = new BufferedLineReader(static function (string $line) {
                $this->logger->info($line);
            }, "\n");
            pipe($process->getStdout(), $stdOutReader);
            $stdErrReader = new BufferedLineReader(static function (string $line) {
                $this->logger->error($line);
            }, "\n");
            pipe($process->getStderr(), $stdErrReader);
        });
    }

    protected function getArguments(): array
    {
        $sockMode = '0660';
        // $sockGroup = 'icingaweb2';
        return [
            '-B', // Only permit writes into the base directory specified
            // '-L', // NETWORK_OPTIONS
            '-b', $this->getDataDirectory(),
            '-R', // Permit recursive subdirectory creation in the base directory (only with -B)
            // NOT setting -F, we want a fast shutdown, not flushing as there is a journal
            '-j', $this->getJournalDirectory(),
            '-p', $this->getPidFile(),
            // Order matters, -m and -s affect FOLLOWING sockets
            // -s  Unix socket group permissions: numeric group id or group name
            '-m', $sockMode,
            // '-s $sockGroup',
            '-l', "unix:" . $this->getSocketFile(),
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
}
