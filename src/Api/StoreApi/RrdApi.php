<?php

namespace IMEdge\MetricsFeature\Api\StoreApi;

use gipfl\RrdTool\AsyncRrdtool;
use gipfl\RrdTool\DsList;
use gipfl\RrdTool\RrdCached\RrdCachedClient;
use gipfl\RrdTool\RrdGraphInfo;
use gipfl\RrdTool\RrdInfo;
use gipfl\RrdTool\RrdSummary;
use IMEdge\RpcApi\ApiMethod;
use IMEdge\RpcApi\ApiNamespace;
use InvalidArgumentException;

use function React\Async\await as reactAwait;
use function React\Promise\all;
use function React\Promise\resolve;

#[ApiNamespace('rrd')]
class RrdApi
{
    protected AsyncRrdtool $rrdtool;
    protected RrdCachedClient $client;

    public function __construct(AsyncRrdtool $rrdtool, RrdCachedClient $client)
    {
        $this->rrdtool = $rrdtool;
        $this->client = $client;
    }

    #[ApiMethod]
    public function graph(string $format, string $command): array
    {
        $rrdtool = $this->rrdtool;
        $start = microtime(true);
        /*
        $binary = '/usr/bin/rrdtool';
        $basedir = '/shared/containers/web/rrd';
        $rrdtool = new Rrdtool("$basedir/data", $binary, "$basedir/rrdcached.sock");
        */
        // Used to be $this->graph->getFormat()

// Logger::info(date('Ymd His'.substr((string)microtime(), 1, 8).' e') . 'Running ' . $command);
        return reactAwait($rrdtool->send($command)->then(function ($image) use ($start, $format) {
            $duration = \microtime(true) - $start;
            if (\strlen($image) === 0) {
                throw new \RuntimeException('Got an empty STDOUT');
            }
            // $this->logger->info('Got STDOUT: ' . \strlen($image) . 'Bytes');
            $info = RrdGraphInfo::parseRawImage($image);
            $info['time_spent'] = $duration;
            $imageString = \substr($image, $info['headerLength']);
            $props = $info;
            RrdGraphInfo::appendImageToProps($props, $imageString, $format);
            return $props;
        }));

        // $info['rrdtool_total_time'] = $rrdtool->getTotalSpentTimings();
        // $info['print'] = $graph->translatePrintLabels($info['print']);
// Logger::info(date('Ymd His'.substr((string)microtime(), 1, 8).' e') . 'Result ready');
    }

    #[ApiMethod]
    public function version(): string
    {
        return 'v0.8.0';
    }

    #[ApiMethod]
    public function recreate(string $file): ?string
    {
        return reactAwait($this->rrdtool->recreateFile($file, true));
    }

    #[ApiMethod]
    public function tune(string $file, string $tuning): string
    {
        $rrdtool = $this->rrdtool;

        // string(25) "OK u:0,24 s:0,00 r:31,71 " ??
        return reactAwait($rrdtool->send("tune $file $tuning"));
    }

    #[ApiMethod]
    public function merge(array $files, string $outputFile): array
    {
        $jobs = [];
        if (empty($files)) {
            throw new InvalidArgumentException('Merging requires at least one file');
        }
        $cmdSuffix = '';
        foreach ($files as $file) {
            $jobs[$file] = $this->client->info($file);
            $cmdSuffix .= " --source $file";
        }
        return reactAwait(all($jobs)->then(function ($infos) use ($outputFile, $cmdSuffix) {
            /** @var RrdInfo[] $infos */
            $first = current($infos);
            $newDs = new DsList();
            foreach ($infos as $info) {
                foreach ($info->getDsList()->getDataSources() as $ds) {
                    if (! $newDs->hasName($ds->getName())) {
                        $newDs->add($ds);
                    }
                }
            }
            $rra = $first->getRraSet();
            return $this->rrdtool->send("create $outputFile $rra $newDs$cmdSuffix");
        }));
    }

    #[ApiMethod]
    public function flush(string $file): bool
    {
        return reactAwait($this->client->flush($file));
    }

    #[ApiMethod]
    public function forget(string $file): bool
    {
        return reactAwait($this->client->flush($file));
    }

    #[ApiMethod]
    public function flushAndForget(string $file): bool
    {
        return reactAwait($this->client->flushAndForget($file));
    }

    #[ApiMethod]
    public function delete(string $file): bool
    {
        return reactAwait($this->client->forget($file)
            ->then(function () use ($file) {
                // This blocks:
                @unlink($this->rrdtool->getBasedir() . '/' . $file);
                return true;
            }));
    }

    #[ApiMethod]
    public function info(string $file): RrdInfo
    {
        return reactAwait($this->client->info($file));
    }

    #[ApiMethod]
    public function rawinfo(string $file): string
    {
        return reactAwait($this->client->rawInfo($file));
    }

    #[ApiMethod]
    public function pending(string $file): array
    {
        return reactAwait($this->client->pending($file));
    }

    #[ApiMethod]
    public function first(string $file, int $rra = 0): int
    {
        return reactAwait($this->client->first($file, $rra));
    }

    #[ApiMethod]
    public function last(string $file): int
    {
        return reactAwait($this->client->last($file));
    }

    /**
     * @return int[]
     */
    #[ApiMethod]
    public function multiLast(array $files): array
    {
        $result = [];
        foreach ($files as $file) {
            $result[$file] = $this->client->last($file);
        }

        return reactAwait(all($result));
    }

    #[ApiMethod]
    public function calculate(array $files, array $dsNames, int $start, int $end): array
    {
        $ds = [];
        foreach ($files as $file) {
            foreach ($dsNames as $name) {
                $ds[] = (object)[
                    'filename'   => $file,
                    'datasource' => $name,
                ];
            }
        }

        $summary = new RrdSummary($this->rrdtool);

        return reactAwait($summary->summariesForDatasources($ds, $start, $end)->then(function ($result) {
            return $result;
        }));
    }

    #[ApiMethod]
    public function listCommands(): array
    {
        return reactAwait($this->client->listAvailableCommands());
    }

    #[ApiMethod]
    public function hasCommands(string $command): bool
    {
        return reactAwait($this->client->hasCommand($command));
    }

     // TODO: Parameter $path, check glob
    #[ApiMethod('listRequest')] // TODO: Api method name different from method name fails in ApiRunner::addApi()
    public function listRequest(): array
    {
        return reactAwait($this->client->hasCommand('LIST')->then(function ($hasList) {
            if ($hasList) {
                return $this->client->listFiles();
            }
            $basedir = $this->rrdtool->getBasedir();
            $prefixLen = \strlen($basedir) + 1;
            return resolve(\array_map(static function ($file) use ($prefixLen) {
                return \substr($file, $prefixLen);
            }, \glob($basedir . '/*')));
        }));
    }

    // TODO: Parameter $path, check glob
    #[ApiMethod]
    public function listRecursive(): array
    {
        return reactAwait($this->client->hasCommand('LIST')->then(function ($hasList) {
            if ($hasList) {
                return $this->client->listRecursive();
            }
            $basedir = $this->rrdtool->getBasedir();
            $prefixLen = \strlen($basedir) + 1;
            return resolve(\array_map(static function ($file) use ($prefixLen) {
                return \substr($file, $prefixLen);
            }, \glob($basedir . '/**/*.rrd')));
        }));
    }
}
