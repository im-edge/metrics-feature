<?php

namespace IcingaMetrics;

use gipfl\IcingaPerfData\Ci;
use gipfl\RrdTool\DsList;
use gipfl\RrdTool\RraSet;
use gipfl\ZfDb\Adapter\Adapter;
use Psr\Log\LoggerInterface;
use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidInterface;

class DbInventory
{
    protected Adapter $db;
    protected LoggerInterface $logger;

    public function __construct(Adapter $db, LoggerInterface $logger)
    {
        $this->db = $db;
        $this->logger = $logger;
    }

    public function registerDataNode(DataNode $node): bool
    {
        return $this->storeWithUuid($node->getUuid(), 'datanode', [
            'label' => $node->getName()
        ], 'label');
    }

    public function registerMetricsStore(MetricStore $store): bool
    {
        return $this->storeWithUuid($store->getUuid(), 'metric_store', [
            'label'         => $store->getName(),
            'datanode_uuid' => $store->getDataNodeUuid()->getBytes(),
            'basedir'       => $store->getBaseDir(),
            'rrdtool_path'  => '/usr/bin',
            'redis_path'    => '/usr/bin',
            'username'      => 'still-unknown',
        ], 'label');
    }

    public function getRegisteredCiUuid(Ci $ci): UuidInterface
    {
        $table = 'ci';
        $checksum = $ci->calculateChecksum();
        $row = $this->fetchOptional($table, 'checksum', $checksum);
        if ($row) {
            return Uuid::fromBytes($row['uuid']);
        }

        $uuid = Uuid::uuid4();
        $this->db->insert($table, [
            'uuid' => $uuid->getBytes(),
            'checksum' => $checksum,
            'hostname' => $ci->getHostname(),
            'subject'  => $ci->getSubject(),
            'instance' => $ci->getInstance(),
        ]);

        return $uuid;
    }

    public function registerFile(
        UuidInterface $uuid,
        Ci            $ci,
        UuidInterface $storeUuid,
        string        $filename,
        int           $step,
        DsList        $dsList,
        RraSet        $rraSet
    ): bool {
        // TODO: We might want to skip fetching checksums in case they do not modify.
        //       Our abstraction currently doesn't allow doing so
        return $this->storeWithUuid($uuid, 'rrd_file', [
            'metric_store_uuid' => $storeUuid->getBytes(),
            'ci_uuid'  => $this->getRegisteredCiUuid($ci)->getBytes(),
            'rrd_step' => $step,
            'filename' => $filename,
            'rrd_archive_set_checksum'     => $this->getRegisteredRraSetChecksum($rraSet),
            'rrd_datasource_list_checksum' => $this->getRegisteredDsListChecksum($dsList),
            'deleted' => 'n',
        ]);
    }

    protected function fetchOptional($table, $keyColumn, $key): ?array
    {
        $db = $this->db;
        $row = $db->fetchRow($db->select()->from($table)->where("$keyColumn = ?", $key));
        if ($row) {
            $row = (array) $row;
            unset($row[$keyColumn]);
            return $row;
        }

        return null;
    }

    protected function fetchOptionalRowByUuid(UuidInterface $uuid, $table): ?array
    {
        return $this->fetchOptional($table, 'uuid', $uuid->getBytes());
    }

    protected function storeWithUuid(UuidInterface $uuid, $table, $properties, $uniqueKey = null): bool
    {
        $row = $this->fetchOptionalRowByUuid($uuid, $table);
        $db = $this->db;
        if ($row) {
            $modified = $this->getPropertyDiff($row, $properties);
            if (empty($modified)) {
                // No changes to apply
                return false;
            } elseif ($db->update($table, $modified, $db->quoteInto('uuid = ?', $uuid->getBytes()))) {
                return true;
            } else {
                // DB row did not change
                return false;
            }
        } else {
            if ($uniqueKey !== null) {
                $this->assertUniqueKeyDoesNotExist($table, $uniqueKey, $properties[$uniqueKey], $uuid);
            }
            $db->insert($table, ['uuid' => $uuid->getBytes()] + $properties);
            return true;
        }
    }

    protected function getPropertyDiff(array $left, array $right): array
    {
        $modified = [];
        foreach ($right as $key => $value) {
            if ($left[$key] !== $value) {
                $modified[$key] = $value;
            }
        }

        return $modified;
    }

    protected function assertUniqueKeyDoesNotExist($table, $key, $value, UuidInterface $myUuid)
    {
        $otherUuid = $this->db->fetchOne(
            $this->db->select()->from($table, 'uuid')->where("$key = ?", $value)
        );

        if ($otherUuid) {
            throw new \RuntimeException(sprintf(
                'Cannot claim %s %s named "%s" for %s, already assigned to %s',
                $table,
                $key,
                $value,
                $myUuid->toString(),
                Uuid::fromBytes($otherUuid)->toString()
            ));
        }
    }

    public function getRegisteredRraSetChecksum(RraSet $set): string
    {
        $checksum = sha1((string) $set, true);
        if ($this->hasRraSetChecksum($checksum)) {
            return $checksum;
        }

        return $this->storeRraSet($set);
    }

    protected function storeRraSet(RraSet $set): string
    {
        $checksum = sha1((string) $set, true);
        $db = $this->db;
        try {
            $db->beginTransaction();
            $db->insert('rrd_archive_set', [
                'checksum' => $checksum
            ]);

            foreach ($set->getRras() as $idx => $rra) {
                $db->insert('rrd_archive', [
                    'rrd_archive_set_checksum' => $checksum,
                    'rra_index'                => $idx,
                    'consolidation_function'   => $rra->getConsolidationFunction(),
                    'row_count'                => $rra->getRows(), // ??
                    'settings'                 => $rra->toString(),
                    'defer_creation'           => 'n', // TODO: How to handle this?
                ]);
            }
            $db->commit();
        } catch (\Exception $e) {
            $this->logger->error($e->getMessage());
            try {
                $db->rollBack();
            } catch (\Exception $e) {
                // ...
            }
            throw $e;
        }

        return $checksum;
    }

    public function hasRraSetChecksum(string $checksum): bool
    {
        return $this->db->fetchOne('SELECT checksum FROM rrd_archive_set WHERE checksum = ?', $checksum) !== false;
    }

    public function getRegisteredDsListChecksum(DsList $dsList): string
    {
        $checksum = sha1((string) $dsList, true);
        if ($this->hasDsListChecksum($checksum)) {
            return $checksum;
        }

        return $this->storeDsList($dsList);
    }

    protected function storeDsList(DsList $list): string
    {
        $checksum = sha1((string) $list, true);
        $db = $this->db;
        try {
            $db->beginTransaction();
            $db->insert('rrd_datasource_list', [
                'checksum' => $checksum
            ]);

            $idx = 0;
            foreach ($list->getDatasources() as $name => $ds) {
                $idx++;
                $db->insert('rrd_datasource', [
                    'datasource_list_checksum' => $checksum,
                    'datasource_index'  => $idx,
                    'datasource_name'   => $ds->getName(),
                    'datasource_type'   => $ds->getType(),
                    'datasource_label'  => $ds->getName(), // TODO: LABEL!!!
                    'minimal_heartbeat' => $ds->getHeartbeat(),
                    'min_value' => $ds->getMin(),
                    'max_value' => $ds->getMax(),
                ]);
            }
            $db->commit();
        } catch (\Exception $e) {
            try {
                $this->logger->error($e->getMessage());
                $db->rollBack();
            } catch (\Exception $e) {
                // ...
            }
            throw $e;
        }

        return $checksum;
    }

    public function hasDsListChecksum(string $checksum): bool
    {
        return $this->db->fetchOne('SELECT checksum FROM rrd_datasource_list WHERE checksum = ?', $checksum) !== false;
    }
}
