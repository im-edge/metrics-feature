<?php

namespace IMEdge\MetricsFeature\FileInventory;

use gipfl\RrdTool\AsyncRrdtool;
use gipfl\RrdTool\DsList;
use gipfl\RrdTool\RraSet;
use gipfl\RrdTool\RrdCached\RrdCachedClient;
use gipfl\RrdTool\RrdInfo;
use gipfl\RrdTool\SampleRraSet;
use IMEdge\Filesystem\Directory;
use Psr\Log\LoggerInterface;
use React\Promise\PromiseInterface;

use function addcslashes;

class RrdFileStore
{
    protected bool $rrdCachedHasTuneCommand = true;
    protected RraSet $defaultRraSet;

    public function __construct(
        protected readonly RrdCachedClient $rrdCached,
        protected readonly AsyncRrdtool $rrdTool,
        protected readonly LoggerInterface $logger
    ) {
        // alternative: SampleRraSet::kickstartWithSeconds();
        $this->defaultRraSet = SampleRraSet::pnpDefaults();
    }

    /**
     * @return PromiseInterface<RrdInfo>
     */
    public function createOrTweak(string $filename, DsList $dsList, int $step, int $start): PromiseInterface
    {
        // CREATE filename [-s stepsize] [-b begintime] [-O] DSdefinitions ... RRAdefinitions ...
        //
        // This will create the RRD file according to the supplied parameters,
        // provided the parameters are valid, and (if the -O option is given
        // or if the rrdcached was started with the -O flag) the specified
        // filename does not already exist.
        return $this->rrdCached->info($filename)
            ->then(function (RrdInfo $info) use ($filename, $dsList) {
                if ($dsAdd = self::calculateNewDsList($info->getDsList(), $dsList)) {
                    $this->logger->notice(sprintf('Tuning %s: %s', $filename, $dsAdd));

                    if ($this->rrdCachedHasTuneCommand) {
                        return $this->rrdCached->tune($filename, $dsAdd);
                    } else {
                        // Hint: this adds new data sources. You can remove them with DEL:ds1 DEL:ds2
                        $command = sprintf('tune %s %s', $filename, $dsAdd);
                        return $this->rrdTool->send($command);
                    }
                }

                return $info;
            }, function () use ($filename, $step, $start, $dsList) {
                // $this->logger->debug("Creating $filename: $step $start, " . $dsList);
                return $this->createFile($filename, $step, $start, $dsList, $this->defaultRraSet);
            });
    }

    /**
     * @return PromiseInterface<RrdInfo>
     */
    protected function createFile(
        string $filename,
        int $step,
        int $start,
        DsList $dsList,
        RraSet $rraSet = null
    ): PromiseInterface {
        Directory::requireWritable(dirname($filename), true);
        $this->logger->notice(sprintf(
            'create %s -s %d -b %d %s %s',
            addcslashes($filename, ' '), // TODO: check how to escape
            $step,
            $start,
            $dsList,
            $rraSet
        ));
        return $this->rrdTool->send(sprintf(
            'create %s -s %d -b %d %s %s',
            addcslashes($filename, ' '), // TODO: check how to escape
            $step,
            $start,
            $dsList,
            $rraSet
        ))->then(function () use ($filename) {
            return $this->rrdCached->info($filename);
        }, function (\Exception $e) {
            $this->logger->error($e->getMessage());
        });
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
