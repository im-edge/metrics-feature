<?php

namespace iPerGraph;

use gipfl\RrdTool\DsList;
use gipfl\RrdTool\RraSet;
use gipfl\RrdTool\RrdCached\Client as RrdCachedClient;
use gipfl\RrdTool\RrdInfo;
use gipfl\RrdTool\SampleRraSet;
use iPerGraph\NamingStrategy\DefaultNamingStrategy;
use Psr\Log\LoggerInterface;
use React\Promise;
use function array_combine;
use function array_keys;
use function substr;

// TODO: remove echo/print
class Store
{
    /** @var RedisPerfDataApi */
    protected $redisApi;

    /** @var RrdCachedClient */
    protected $rrdCached;

    protected $naming;

    protected $logger;

    public function __construct(RedisPerfDataApi $redisApi, RrdCachedClient $rrdCached, LoggerInterface $logger)
    {
        $this->redisApi = $redisApi;
        $this->logger = $logger;
        $this->naming = new DefaultNamingStrategy($logger);
        $this->rrdCached = $rrdCached;
    }

    /**
     * TODO: I tend to
     *
     * @param $ci
     * @param PerfData $perfData
     * @param $base 1 or 60 -> sec or min
     * @return mixed
     */
    public function createCi($ci, PerfData $perfData, $base)
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
        $cfg = $this->naming->prepareCiConfig($ci, $keyValue);

        // CREATE filename [-s stepsize] [-b begintime] [-O] DSdefinitions ... RRAdefinitions ...
        //
        // This will create the RRD file according to the supplied parameters,
        // provided the parameters are valid, and (if the -O option is given
        // or if the rrdcached was started with the -O flag) the specified
        // filename does not already exist.
        $register = function () use ($ci, $cfg) {
            return $this->redisApi->setCiConfig($ci, $cfg);
        };

        return $this->rrdCached->info($cfg->filename)
            ->then(function (RrdInfo $info) use ($ci, $cfg, $dsList) {
                // echo "Got info for $filename\n";
                $currentNames = \array_flip($info->listDsNames());
                $newDs = [];
                // TODO: compare type and other properties
                foreach ($dsList as $ds) {
                    if (! isset($currentNames[$ds->getName()])) {
                        $newDs[$ds->getName()] = $ds;
                    }
                }
                if (! empty($newDs)) {
                    // $rrdtool->run("tune $file $tuning", false);
                    $this->logger->debug(\sprintf(
                        "Tuning %s (not yet): %s\n",
                        $cfg->filename,
                        \implode(', ', array_keys($newDs))
                    ));
                }

                return $this->redisApi->setCiConfig($ci, $cfg);
            })->otherwise(function () use ($cfg, $step, $start, $dsList, $ci) {
                $filename = $cfg->filename;
                $this->logger->debug("Got NO info for $filename, creating the file\n");
                return $this->createFile($filename, $step, $start, $dsList)->then(function () use ($ci, $cfg) {
                    return $this->redisApi->setCiConfig($ci, $cfg);
                });
            });
    }

    /**
     * @param $filename
     * @param $step
     * @param $start
     * @param DsList $dsList
     * @param RraSet|null $rraSet
     * @return Promise\Promise
     */
    protected function createFile($filename, $step, $start, DsList $dsList, RraSet $rraSet = null)
    {
        if ($rraSet === null) {
            // $rraSet = SampleRraSet::kickstartWithSeconds();
            $rraSet = SampleRraSet::pnpDefaults();
        }

        $cmd = \sprintf(
            "CREATE %s -s %d -b %d %s %s",
            \addcslashes($filename, ' '), // TODO: check how to escape
            $step,
            $start,
            $dsList,
            $rraSet
        );
        $this->logger->debug("$cmd");

        return $this->rrdCached->send($cmd);
    }

    public function deferCi($ci, $filename)
    {
        return $this->redisApi->deferCi($ci, 'manual')->then(function () use ($filename) {
            return $this->rrdCached->flushAndForget($filename);
        });
    }
}
