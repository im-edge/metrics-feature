<?php

/**
 * This is an IMEdge feature
 *
 * @var Feature $this
 */

use IMEdge\Metrics\MetricsEvent;
use IMEdge\MetricsFeature\Api\MainApi\MetricsApi;
use IMEdge\MetricsFeature\FeatureRunner;
use IMEdge\Node\Feature;

$runner = new FeatureRunner($this, $this->logger);
$this->events->on(MetricsEvent::ON_MEASUREMENTS, function ($measurements) use ($runner) {
    $storeName = 'snmp'; // TODO: ?!?!?
    try {
        $runner->shipMeasurements($measurements, $storeName);
    } catch (Throwable $e) {
        $this->logger->error(sprintf('Failed to ship measurement for %s: %s', $storeName, $e->getMessage()));
    }
});
$this->registerRpcApi(new MetricsApi($runner, $this->logger));
$this->onShutdown($runner->stop(...));
$runner->run();
