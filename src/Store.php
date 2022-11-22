<?php

namespace IcingaMetrics;

use gipfl\RrdTool\AsyncRrdtool;
use gipfl\RrdTool\DsList;
use gipfl\RrdTool\RraSet;
use gipfl\RrdTool\RrdCached\Client as RrdCachedClient;
use gipfl\RrdTool\RrdInfo;
use gipfl\RrdTool\SampleRraSet;
use IcingaMetrics\NamingStrategy\DefaultNamingStrategy;
use Psr\Log\LoggerInterface;
use React\Promise\ExtendedPromiseInterface;
use function addcslashes;
use function array_flip;
use function floor;
use function sprintf;
use function substr;

class Store
{
    protected RedisPerfDataApi $redisApi;
    protected RrdCachedClient $rrdCached;
    protected AsyncRrdtool $rrdTool;
    protected DefaultNamingStrategy $naming;
    protected LoggerInterface $logger;

    public function __construct(
        RedisPerfDataApi $redisApi,
        RrdCachedClient $rrdCached,
        AsyncRrdtool $rrdtool,
        LoggerInterface $logger
    ) {
        $this->redisApi = $redisApi;
        $this->logger = $logger;
        $this->rrdTool = $rrdtool;
        $this->naming = new DefaultNamingStrategy($logger);
        $this->rrdCached = $rrdCached;
    }

    /**
     * TODO: I tend to
     *
     * @param $ci
     * @param PerfData $perfData
     * @param $base 1 or 60 -> sec or min
     * @return ExtendedPromiseInterface
     */
    public function wantCi($ci, PerfData $perfData, $base): ExtendedPromiseInterface
    {
        $timestamp = $perfData->getTime();

        // Align start to RRD step
        $start = (int) (floor($timestamp / $base) * $base);

        $keyValue = [];
        foreach ($perfData->getValues() as $key => $value) {
            if (substr($value, -1, 1) === 'c') {
                $type = 'COUNTER';
            } else {
                $type = 'GAUGE';
            }
            $keyValue[$key] = [$type, $value];
        }

        if ($base === 1) {
            $step = 1;
        } else {
            $step = 60;
        }
        $dsList = $this->naming->getDataSourcesFor($keyValue);
        /*
        $dsList = $this->naming->getDataSourcesFor($keyValue);
        $map = array_combine(array_keys($keyValue), $dsList->listNames());
        $filename = $this->naming->getFilename($ci);
        $cfg = (object) [
            'filename' => $filename,
            'dsNames'  => $dsList->listNames(),
            'dsMap'    => $map,
        ];
        */
        $ciConfig = $this->naming->prepareCiConfig($ci, $keyValue);

        // CREATE filename [-s stepsize] [-b begintime] [-O] DSdefinitions ... RRAdefinitions ...
        //
        // This will create the RRD file according to the supplied parameters,
        // provided the parameters are valid, and (if the -O option is given
        // or if the rrdcached was started with the -O flag) the specified
        // filename does not already exist.
        return $this->rrdCached->info($ciConfig->filename)
            ->then(function (RrdInfo $info) use ($ci, $ciConfig, $dsList) {
                $currentNameLookup = array_flip($info->listDsNames());
                $newDs = [];
                // TODO: compare type and other properties
                foreach ($dsList as $ds) {
                    if (! isset($currentNameLookup[$ds->getName()])) {
                        $newDs[$ds->getName()] = $ds;
                    }
                }
                $rraSet = $info->getRraSet();
                if (! empty($newDs)) {
                    $list = new DsList($newDs);
                    $command = sprintf('tune %s %s', $ciConfig->filename, $list);
                    $this->logger->notice(sprintf('Tuning %s: %s', $ciConfig->filename, $list));

                    return $this->rrdTool->send($command)->then(function () use ($ci, $ciConfig, $dsList, $rraSet) {
                        return $this->redisApi->setCiConfig($ci, $ciConfig, $dsList, $rraSet);
                    });
                }

                return $this->redisApi->setCiConfig($ci, $ciConfig, $dsList, $info->getRraSet());
            }, function () use ($ciConfig, $step, $start, $dsList, $ci) {
                $filename = $ciConfig->filename;
                // $this->logger->debug("Creating $filename: $step $start, " . $dsList);
                // $rraSet = SampleRraSet::kickstartWithSeconds();
                $rraSet = SampleRraSet::pnpDefaults();
                return $this->createFile($filename, $step, $start, $dsList, $rraSet)
                    ->then(function () use ($ci, $ciConfig, $dsList, $rraSet) {
                        return $this->redisApi->setCiConfig($ci, $ciConfig, $dsList, $rraSet);
                    });
            });
    }

    /**
     * @param $filename
     * @param $step
     * @param $start
     * @param DsList $dsList
     * @param RraSet|null $rraSet
     * @return ExtendedPromiseInterface
     */
    protected function createFile(
        string $filename,
        int $step,
        int $start,
        DsList $dsList,
        RraSet $rraSet = null
    ): ExtendedPromiseInterface {
        if ($rraSet === null) {
            // $rraSet = SampleRraSet::kickstartWithSeconds();
            $rraSet = SampleRraSet::pnpDefaults();
        }
        $dirName = dirname($filename);
        FilesystemUtil::requireDirectory($dirName, true);

        $cmd = sprintf(
            "create %s -s %d -b %d %s %s",
            addcslashes($filename, ' '), // TODO: check how to escape
            $step,
            $start,
            $dsList,
            $rraSet
        );

        return $this->rrdTool->send($cmd);
    }

    public function deferCi($ci, $filename)
    {
        return $this->redisApi->deferCi($ci, 'manual')->then(function () use ($filename) {
            return $this->rrdCached->flushAndForget($filename);
        });
    }
}
