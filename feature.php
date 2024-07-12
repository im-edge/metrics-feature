<?php

/**
 * This is an IMEdge feature
 *
 * @var Feature $this
 */

use IMEdge\MetricsFeature\Api\MainApi\MetricsApi;
use IMEdge\MetricsFeature\FeatureRunner;
use IMEdge\MetricsFeature\MetricStoreRunner;
use IMEdge\Node\Feature;

require __DIR__ . '/vendor/autoload.php';

$runner = new FeatureRunner($this, $this->logger);
$this->events->on(MetricStoreRunner::ON_MEASUREMENTS, function ($measurements) use ($runner) {
    $storeName = 'lab1'; // TODO: ?!?!?
    try {
        $runner->shipMeasurements($measurements, $storeName);
    } catch (Throwable $e) {
        $this->logger->error(sprintf('Failed to ship measurement for %s: %s', $storeName, $e->getMessage()));
    }
});
$this->registerRpcApi(new MetricsApi($runner, $this->logger));
$this->onShutdown($runner->stop(...));
$runner->run();
