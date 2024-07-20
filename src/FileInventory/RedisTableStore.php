<?php

namespace IMEdge\MetricsFeature\FileInventory;

use Amp\Redis\RedisClient;
use gipfl\Json\JsonString;
use IMEdge\Metrics\Ci;
use IMEdge\Metrics\Measurement;
use IMEdge\MetricsFeature\CiConfig;
use IMEdge\MetricsFeature\Rrd\DsHelper;
use IMEdge\RrdCached\RrdCachedClient;
use IMEdge\RrdStructure\RrdInfo;
use Psr\Log\LoggerInterface;

use function floor;

class RedisTableStore
{
    public function __construct(
        protected readonly RedisClient $redis,
        protected readonly RrdCachedClient $rrdCached,
        protected RrdFileStore $rrdFileStore,
        protected readonly LoggerInterface $logger
    ) {
    }

    /**
     * TODO: I tend to... ?
     *
     * @param int $base 1 or 60 -> sec or min
     */
    public function wantCi(Measurement $measurement, int $base): RrdInfo
    {
        $dsList = DsHelper::getDataSourcesForMeasurement($this->logger, $measurement);
        $ciConfig = CiConfig::forDsList($dsList);

        // Align start to RRD step
        $start = (int) floor($measurement->getTimestamp() / $base) * $base;
        $step = $base === 1 ? 1 : 60;
        $info = $this->rrdFileStore->createOrTweak($ciConfig->filename, $dsList, $step, $start);
        $ciName = JsonString::encode($measurement->ci);
        $this->logger->debug("Registering $ciName in Redis");
        $this->redis->execute('HSET', 'ci', $ciName, JsonString::encode($ciConfig));
        $info->getDsList()->applyAliasMapFromDsList($dsList);
        return $info;
    }

    public function deferCi(Ci $ci, string $filename): bool
    {
        $reason = 'manual';
        $this->redis->execute('HSET', 'deferred-cids', JsonString::encode($ci), JsonString::encode([
            'reason' => $reason,
            'since'  => time()
        ]));

        return $this->rrdCached->flushAndForget($filename);
    }
}
