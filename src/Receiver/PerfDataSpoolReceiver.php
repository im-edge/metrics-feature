<?php

namespace IcingaMetrics\Receiver;

use gipfl\IcingaPerfData\Measurement;
use IcingaMetrics\PerfData;
use IcingaMetrics\PerfDataShipper;
use IcingaMetrics\RedisPerfDataApi;

class PerfDataSpoolReceiver extends BaseReceiver
{
    public function run()
    {
        $redis = new RedisPerfDataApi($this->logger, $this->metricStore->getRedisSocketUri());
        $redis->setClientName('IcingaMetrics::perfdataShipper');
        $perf = new PerfDataShipper($this->logger, $this->settings->getRequired('spool-directory'));
        $redis->on(RedisPerfDataApi::ON_STRAIN_START, function ($count) use ($perf) {
            $this->logger->notice(sprintf('%d items waiting for Redis, pause reading', $count));
            $perf->pause();
        });
        $redis->on(RedisPerfDataApi::ON_STRAIN_END, function ($count) use ($perf) {
            $this->logger->notice(sprintf('%d items waiting for Redis, resume reading', $count));
            $perf->resume();
        });
        $perf->on(PerfDataShipper::ON_MEASUREMENT, function (Measurement $measurement) use ($redis) {
            $redis->shipPerfData(PerfData::fromMeasurement($measurement));
        });
        $perf->on(PerfDataShipper::ON_MEASUREMENTS, function (array $measurements) use ($redis) {
            $perfData = [];
            foreach ($measurements as $measurement) {
                $perfData[] = PerfData::fromMeasurement($measurement);
            }
            $redis->shipBulkPerfData($perfData)->done();
        });
        $perf->run();
    }
}
