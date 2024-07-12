<?php

namespace IMEdge\MetricsFeature;

use IMEdge\Node\UtilityClasses\DirectoryBasedComponent;
use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidInterface;
use RuntimeException;

class MetricStore
{
    use DirectoryBasedComponent;

    protected const DOT_DIR = '.icinga-metrics';
    protected const CONFIG_TYPE = 'Metrics/Store';
    protected const CONFIG_FILE_NAME = 'metric-store.json';
    protected const CONFIG_VERSION = 'v1';
    protected const SUPPORTED_CONFIG_VERSIONS = [
        self::CONFIG_VERSION,
    ];

    public function getNodeUuid(): ?UuidInterface
    {
        if ($this->config === null) {
            return null;
        }
        $uuid = $this->config->get('datanode');
        if ($uuid) {
            return Uuid::fromString($uuid);
        }

        return null;
    }

    public function getRedisBaseDir(): string
    {
        return $this->getBaseDir() . '/redis';
    }

    public function getRedisSocketPath(): string
    {
        return $this->getRedisBaseDir() . '/redis.sock';
    }

    public function setNodeUuid(UuidInterface $uuid, bool $force = false): void
    {
        $currentUuid = $this->getNodeUuid();
        if ($currentUuid) {
            if ($currentUuid->equals($uuid)) {
                return;
            }
            if (!$force) {
                throw new RuntimeException(sprintf(
                    'Cannot claim Metric Store "%s" for %s, it belongs to %s',
                    $this->getName(),
                    $uuid->toString(),
                    $currentUuid->toString()
                ));
            }
        }

        $this->requireConfig()->set('datanode', $uuid->toString());
        $this->storeConfig($this->config);
    }
}
