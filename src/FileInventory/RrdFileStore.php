<?php

namespace IMEdge\MetricsFeature\FileInventory;

use IMEdge\Filesystem\Directory;
use IMEdge\MetricsFeature\Rrd\RrdtoolRunner;
use IMEdge\MetricsFeature\Rrd\SingleShotRunner;
use IMEdge\RrdCached\RrdCachedClient;
use IMEdge\RrdStructure\DsList;
use IMEdge\RrdStructure\RraSet;
use IMEdge\RrdStructure\RrdInfo;
use Psr\Log\LoggerInterface;

use function addcslashes;

class RrdFileStore
{
    protected bool $rrdCachedHasTuneCommand = true;
    protected RraSet $defaultRraSet;
    protected SingleShotRunner $singleShot;

    public function __construct(
        protected readonly RrdCachedClient $rrdCached,
        protected readonly RrdtoolRunner $rrdTool,
        protected readonly LoggerInterface $logger
    ) {
        // alternative: SampleRraSet::kickstartWithSeconds();
        $this->singleShot = new SingleShotRunner($logger);
        $this->defaultRraSet = SampleRraSet::full();
    }

    public function createOrTweak(string $filename, DsList $dsList, int $step, int $start): RrdInfo
    {
        // CREATE filename [-s stepsize] [-b begintime] [-O] DSdefinitions ... RRAdefinitions ...
        //
        // This will create the RRD file according to the supplied parameters,
        // provided the parameters are valid, and (if the -O option is given
        // or if the rrdcached was started with the -O flag) the specified
        // filename does not already exist.
        try {
            // TODO: return ?RrdInfo, and no error?
            $info = $this->rrdCached->info($filename);
        } catch (\Exception $e) {
            // $this->logger->debug("Creating $filename: $step $start, " . $dsList);
            return $this->createFile($filename, $step, $start, $dsList, $this->defaultRraSet);
        }
        if ($dsAdd = self::calculateNewDsList($info->getDsList(), $dsList)) {
            $this->logger->notice(sprintf('Tuning %s: %s', $filename, $dsAdd));

            if ($this->rrdCachedHasTuneCommand) {
                $this->rrdCached->tune($filename, $dsAdd);
            } else {
                // Hint: this adds new data sources. You can remove them with DEL:ds1 DEL:ds2
                $this->rrdTool->send(sprintf('tune %s %s', $filename, $dsAdd));
            }

            return $this->rrdCached->info($filename);
        }

        return $info;
    }

    protected function createFile(
        string $filename,
        int $step,
        int $start,
        DsList $dsList,
        RraSet $rraSet = null
    ): RrdInfo {
        Directory::requireWritable(dirname($filename), true);
        $command = sprintf(
            'create %s -s %d -b %d %s %s',
            addcslashes($filename, ' '), // TODO: check how to escape
            $step,
            $start,
            $dsList,
            $rraSet
        );
        if (strlen($command) < 4000) {
            $this->rrdTool->send($command);
        } else {
            $this->logger->notice(sprintf(
                'Long command, running dedicated process: (%s); %s %s',
                $this->rrdTool->getWorkingDirectory(),
                $this->rrdTool->binary,
                $command
            ));
            $this->singleShot->run($this->rrdTool->binary . ' ' . $command, $this->rrdTool->getWorkingDirectory());
        }

        return $this->rrdCached->info($filename);
    }

    protected static function calculateNewDsList(DsList $left, DsList $right): ?DsList
    {
        $currentNameLookup = array_flip($left->listNames());
        $newDs = [];
        // TODO: compare type and other properties
        foreach ($right->getDataSources() as $ds) {
            if (!isset($currentNameLookup[$ds->getName()])) {
                $newDs[$ds->getName()] = $ds;
            }
        }
        if (empty($newDs)) {
            return null;
        }

        return new DsList($newDs);
    }
}
