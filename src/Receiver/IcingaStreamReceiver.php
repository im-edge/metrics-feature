<?php

namespace IcingaMetrics\Receiver;

use IcingaMetrics\Application;
use IcingaMetrics\IcingaStreamer;
use IcingaMetrics\RedisPerfDataApi;

class IcingaStreamReceiver extends BaseReceiver
{
    public function run()
    {
        $redis = new RedisPerfDataApi($this->logger, $this->metricStore->getRedisSocketUri());
        $redis->setClientName(Application::PROCESS_NAME . '::icinga-stream');
        $icinga = new IcingaStreamer($this->logger, $this->settings);
        $icinga->on(RedisPerfDataApi::ON_PERF_DATA, [$redis, 'shipPerfData']);
    }
}
