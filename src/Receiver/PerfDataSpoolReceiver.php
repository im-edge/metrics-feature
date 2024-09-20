<?php

namespace IMEdge\MetricsFeature\Receiver;

use IMEdge\Json\JsonString;
use IMEdge\Metrics\MetricsEvent;
use IMEdge\MetricsFeature\ApplicationFeature;
use IMEdge\MetricsFeature\PerfDataShipper;
use IMEdge\RedisUtils\LuaScriptRunner;
use IMEdge\RedisUtils\RedisResult;

use function Amp\Redis\createRedisClient;

class PerfDataSpoolReceiver extends BaseReceiver
{
    public function run(): void
    {
        $redis = createRedisClient('unix://' . $this->metricStore->getRedisSocketPath());
        $redis->execute('CLIENT', 'SETNAME', ApplicationFeature::PROCESS_NAME . '::perfdataShipper');
        $perf = new PerfDataShipper(
            $this->logger,
            $this->metricStore->getUuid(),
            $this->settings->getRequired('spool-directory')
        );
        $lua = new LuaScriptRunner($redis, dirname(__DIR__, 2) . '/lua', $this->logger);
        $perf->on(MetricsEvent::ON_MEASUREMENTS, function (array $measurements) use ($lua) {
            $result = RedisResult::toHash(
                $lua->runScript('shipMeasurements', array_map(JsonString::encode(...), $measurements))
            );
        });
        $perf->run();
    }
}
