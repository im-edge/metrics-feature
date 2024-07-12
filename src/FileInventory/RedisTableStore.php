<?php

namespace IMEdge\MetricsFeature\FileInventory;

use Amp\Redis\RedisClient;
use gipfl\Json\JsonString;
use gipfl\RrdTool\AsyncRrdtool;
use gipfl\RrdTool\RrdCached\RrdCachedClient;
use gipfl\RrdTool\RrdInfo;
use IMEdge\Metrics\Ci;
use IMEdge\Metrics\Measurement;
use IMEdge\MetricsFeature\CiConfig;
use IMEdge\MetricsFeature\DsHelper;
use Psr\Log\LoggerInterface;
use React\Promise\PromiseInterface;

use function floor;
use function React\Async\await as awaitReact;

class RedisTableStore
{
    protected RrdFileStore $rrdFileStore;

    public function __construct(
        protected readonly RedisClient $redis,
        protected readonly RrdCachedClient $rrdCached,
        protected readonly AsyncRrdtool $rrdTool,
        protected readonly LoggerInterface $logger
    ) {
        $this->rrdFileStore = new RrdFileStore($this->rrdCached, $this->rrdTool, $this->logger);
    }

    /**
     * TODO: I tend to... ?
     *
     * @param int $base 1 or 60 -> sec or min
     */
    public function wantCi(Measurement $measurement, int $base): RrdInfo
    {
        $keyValue = [];
        foreach ($measurement->getMetrics() as $key => $metric) {
            $keyValue[$key] = [$metric->type, $metric->value];
        }

        $dsList = DsHelper::getDataSourcesForMeasurement($this->logger, $measurement);
        $ciConfig = CiConfig::forDsList($dsList);

        // Align start to RRD step
        $start = (int) floor($measurement->getTimestamp() / $base) * $base;
        $step = $base === 1 ? 1 : 60;
        return awaitReact($this->rrdFileStore->createOrTweak($ciConfig->filename, $dsList, $step, $start)
            ->then(function (RrdInfo $info) use ($measurement, $ciConfig, $dsList) {
                $ciName = JsonString::encode($measurement->ci);
                $this->logger->debug("Registering $ciName in Redis");
                $this->redis->execute('HSET', 'ci', $ciName, JsonString::encode($ciConfig));
                $info->getDsList()->applyAliasMapFromDsList($dsList);
                return $info;
            }));
    }

    public function deferCi(Ci $ci, $filename): PromiseInterface
    {
        $reason = 'manual';
        $this->redis->execute('HSET', 'deferred-cids', JsonString::encode($ci), JsonString::encode([
            'reason' => $reason,
            'since'  => time()
        ]));

        return $this->rrdCached->flushAndForget($filename);
    }
}
