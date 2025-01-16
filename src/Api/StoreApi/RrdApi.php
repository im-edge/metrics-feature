<?php

namespace IMEdge\MetricsFeature\Api\StoreApi;

use IMEdge\MetricsFeature\Rrd\RrdSummary;
use IMEdge\MetricsFeature\Rrd\RrdtoolRunner;
use IMEdge\RpcApi\ApiMethod;
use IMEdge\RpcApi\ApiNamespace;
use IMEdge\RrdCached\RrdCachedClient;
use IMEdge\RrdGraphInfo\GraphInfo;
use IMEdge\RrdStructure\DsList;
use IMEdge\RrdStructure\RrdInfo;
use InvalidArgumentException;
use Psr\Log\LoggerInterface;

#[ApiNamespace('rrd')]
class RrdApi
{
    public function __construct(
        protected RrdtoolRunner $rrdtool,
        protected RrdCachedClient $client,
        protected LoggerInterface $logger,
    ) {
    }

    #[ApiMethod]
    public function graph(string $format, string $command): GraphInfo
    {
        $start = hrtime(true);
        $image = $this->rrdtool->send($command);
        $duration = (hrtime(true) - $start) / 1_000_000_000;

        return GraphInfo::parse($image, $format, $duration, $this->logger);
    }

    #[ApiMethod]
    public function version(): string
    {
        return 'v0.8.0';
    }

    #[ApiMethod]
    public function recreate(string $file): ?string
    {
        return $this->rrdtool->recreateFile($file, true);
    }

    #[ApiMethod]
    public function tune(string $file, string $tuning): string
    {
        $rrdtool = $this->rrdtool;

        // string(25) "OK u:0,24 s:0,00 r:31,71 " ??
        return $rrdtool->send("tune $file $tuning");
    }

    #[ApiMethod]
    public function merge(array $files, string $outputFile): array
    {
        $infos = [];
        if (empty($files)) {
            throw new InvalidArgumentException('Merging requires at least one file');
        }
        $cmdSuffix = '';
        foreach ($files as $file) {
            // TODO: Run in parallel? Does it matter?
            $infos[$file] = $this->client->info($file);
            $cmdSuffix .= " --source $file";
        }
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
    }

    #[ApiMethod]
    public function flushAll(): bool
    {
        return $this->client->flushAll();
    }

    #[ApiMethod]
    public function flush(string $file): bool
    {
        return $this->client->flush($file);
    }

    #[ApiMethod]
    public function forget(string $file): bool
    {
        return $this->client->flush($file);
    }

    #[ApiMethod]
    public function flushAndForget(string $file): bool
    {
        return $this->client->flushAndForget($file);
    }

    #[ApiMethod]
    public function delete(string $file): bool
    {
        $this->client->forget($file);
        @unlink($this->rrdtool->baseDir . '/' . $file);
        return true;
    }

    #[ApiMethod]
    public function info(string $file): RrdInfo
    {
        return $this->client->info($file);
    }

    #[ApiMethod]
    public function rawinfo(string $file): array
    {
        return $this->client->rawInfo($file);
    }

    #[ApiMethod]
    public function pending(string $file): array
    {
        return $this->client->pending($file);
    }

    #[ApiMethod]
    public function first(string $file, int $rra = 0): int
    {
        return $this->client->first($file, $rra);
    }

    #[ApiMethod]
    public function last(string $file): int
    {
        return $this->client->last($file);
    }

    /**
     * @return int[]
     */
    #[ApiMethod]
    public function multiLast(array $files): array
    {
        $result = [];
        foreach ($files as $file) {
            // TODO: is it an advantage, to launch them in parallel?
            $result[$file] = $this->client->last($file);
        }

        return $result;
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
        return $summary->summariesForDatasources($ds, [
            'customPeriod' => [$start, $end]
        ], $this->logger);
    }

    #[ApiMethod]
    public function listCommands(): array
    {
        return $this->client->listAvailableCommands();
    }

    #[ApiMethod]
    public function hasCommands(string $command): bool
    {
        return $this->client->hasCommand($command);
    }

     // TODO: Parameter $path, check glob
    #[ApiMethod('listRequest')] // TODO: Api method name different from method name fails in ApiRunner::addApi()
    public function listRequest(): array
    {
        if ($this->client->hasCommand('LIST')) {
            return $this->client->listFiles();
        }
        $basedir = $this->rrdtool->baseDir;
        $prefixLen = \strlen($basedir) + 1;
        return \array_map(static function ($file) use ($prefixLen) {
            return \substr($file, $prefixLen);
        }, \glob($basedir . '/*'));
    }

    // TODO: Parameter $path, check glob
    #[ApiMethod]
    public function listRecursive(): array
    {
        if ($this->client->hasCommand('LIST')) {
            return $this->client->listRecursive();
        }
        $basedir = $this->rrdtool->baseDir;
        $prefixLen = \strlen($basedir) + 1;
        return \array_map(static function ($file) use ($prefixLen) {
            return \substr($file, $prefixLen);
        }, \glob($basedir . '/**/*.rrd'));
    }
}
