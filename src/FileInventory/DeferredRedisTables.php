<?php

namespace IMEdge\MetricsFeature\FileInventory;

use Amp\Redis\RedisClient;
use gipfl\Json\JsonString;
use IMEdge\Metrics\Ci;
use IMEdge\Metrics\Measurement;
use IMEdge\MetricsFeature\CiConfig;
use IMEdge\RedisTables\RedisTables;
use IMEdge\RedisUtils\LuaScriptRunner;
use IMEdge\RedisUtils\RedisResult;
use Psr\Log\LoggerInterface;
use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidInterface;
use Revolt\EventLoop;

use function Amp\async;
use function Amp\Future\awaitAll;
use function count;
use function key;

/**
 * This fetches pending new CIs or CIs with missing DSs in batches, but processes
 * only one at a time to avoid blocking
 */
class DeferredRedisTables
{
    protected const NS_RRD_DEFINITION = '2e012390-58f9-4e84-8d15-ac61fec61ff1';
    /** @var array<string, Measurement> Key is the JSON-encoded CI definition */
    protected array $pendingCi = [];
    /** @var array<string, Measurement> Key is the JSON-encoded CI definition */
    protected array $pendingDs = [];
    protected UuidInterface $nsRrdDefinition;
    //private string $nodeIdentifier;
    protected ?string $timer = null;
    protected LuaScriptRunner $lua;
    private string $metricStoreIdentifier;
    private string $prefix = 'metrics:';

    // TODO
    // Infrastructure needs to be always ready,
    // if one of them fails eventually keep fetching stats
    // from the others, but stop working and get into failed
    // state. Or exit and let the parent process deal with this;
    public function __construct(
        // NodeIdentifier $nodeIdentifier,
        UuidInterface $metricStoreUuid, // TODO: we do NOT get the metricstoreuuid!!!
        protected readonly RedisClient $redis,
        protected readonly RedisTables $tables,
        protected RedisTableStore $store,
        protected readonly LoggerInterface $logger
    ) {
        $this->lua = new LuaScriptRunner($this->redis, dirname(__DIR__, 2) . '/lua', $this->logger);
        $this->nsRrdDefinition = Uuid::fromString(self::NS_RRD_DEFINITION);
        // $this->nodeIdentifier = $nodeIdentifier->uuid->toString();
        $this->metricStoreIdentifier = $metricStoreUuid->toString();
    }

    public function run(): void
    {
        $this->timer = EventLoop::repeat(1, $this->runCheckForDeferred(...));
    }

    public function stop(): void
    {
        if ($this->timer) {
            EventLoop::cancel($this->timer);
            $this->timer = null;
        }
    }

    protected function runCheckForDeferred(): void
    {
        // Avoiding InvalidCallbackError.php:
        // Non-null return value received from callback class:method defined in file:line
        $this->checkForDeferred();
    }

    protected function getAllAsArray(RedisClient $redis, string $suffix): array
    {
        return RedisResult::toArray($redis->execute('HGETALL', $this->prefix . $suffix));
    }

    public function fetchDeferred(): array
    {
        return awaitAll([
            'missing-ci' => async(fn () => $this->getAllAsArray($this->redis, 'missing-ci')),
            'missing-ds' => async(fn () => $this->getAllAsArray($this->redis, 'missing-ds')),
        ])[1];
    }

    protected function checkForDeferred(): bool
    {

        if (! empty($this->pendingCi)) {
            $this->logger->debug(sprintf('There are still %d items pending:', count($this->pendingCi)));
            return false;
        }
        $missing = $this->fetchDeferred();
        $cntMissingCi = 0;
        foreach ($missing['missing-ci'] ?? [] as $ciString => $measurementString) {
            $this->logger->notice('String: ' . $ciString);
            try {
                $measurement = Measurement::fromSerialization(JsonString::decode($measurementString));
                $this->pendingCi[$ciString] = $measurement;
                $cntMissingCi++;
            } catch (\Throwable $e) {
                $this->logger->error('Failed to handle Missing CI: ' . $e->getMessage());
            }
        }
        $cntMissingDs = 0;
        foreach ($missing['missing-ds'] ?? [] as $ciString => $measurementString) {
            continue;
            try {
                $measurement = Measurement::fromSerialization(JsonString::decode($measurementString));
                $this->pendingDs[$ciString] = $measurement;
                $cntMissingDs++;
            } catch (\Exception $e) {
                $this->logger->error($e->getMessage());
            }
        }
        if ($cntMissingCi === 0 && $cntMissingDs === 0) {
            return true;
        }
        $cntDeferred = 0;
        $cntPending = count($this->pendingCi);
        if ($cntPending > 0) {
            if ($cntDeferred === $cntPending) { // TODO: check this
                $this->logger->debug(sprintf('%d deferred CIs ready to process', $cntPending));
            } else {
                $this->logger->debug(sprintf(
                    '%d out of %s deferred CIs ready to process',
                    $cntPending,
                    $cntDeferred
                ));
            }
            EventLoop::queue($this->processDeferredMeasurement(...));
        }

        return true;
    }

    protected function processDeferredMeasurement(): void
    {
        $ciKey = key($this->pendingCi);
        if ($ciKey === null) {
            return;
        }
        $measurement = $this->pendingCi[$ciKey];

        $base = 60; // 60 seconds base for now
        $info = $this->store->wantCi($measurement, $base);
        $ci = $measurement->ci;
        $ciLogName = self::getCiLogName($ci, $ciKey);
        // TODO: check whether ciLogName equals ciKey
        $this->logger->debug("DeferredHandler: rescheduling all entries for $ciLogName");
        $ns = $this->nsRrdDefinition;
        $rraSetUuidHex = Uuid::uuid5($ns, $info->getRraSet())->toString();
        // TODO: setTableEntries? Batch!
        $futures = [];
        $futures[] = async(fn () => $this->tables->setTableEntry('rrd_archive_set', $rraSetUuidHex, ['uuid'], [
            'uuid' => $rraSetUuidHex
        ]));
        foreach ($info->getRraSet()->getRras() as $rraIndex => $rra) {
            $futures[] = async(fn () => $this->tables->setTableEntry('rrd_archive', "$rraSetUuidHex/$rraIndex", [
                'rrd_archive_set_uuid',
                'rra_index',
            ], [
                'rrd_archive_set_uuid'   => $rraSetUuidHex,
                'rra_index'              => $rraIndex,
                'consolidation_function' => $rra->getConsolidationFunction(),
                'row_count'              => $rra->getRows(),
                'definition'             => (string) $rra,
            ]));
        }
        $dsListUuidHex = Uuid::uuid5($ns, $info->getDsList())->toString();
        $futures[] = async(fn () => $this->tables->setTableEntry('rrd_datasource_list', $dsListUuidHex, ['uuid'], [
            'uuid' => $dsListUuidHex
        ]));

        // RrdInfo has applied aliase!!
        foreach ($info->getDsList()->getDataSources() as $dsIndex => $ds) {
            $futures[] = async(fn () => $this->tables->setTableEntry('rrd_datasource', "$dsListUuidHex/$dsIndex", [
                'datasource_list_uuid',
                'datasource_index',
            ], [
                'datasource_list_uuid' => $dsListUuidHex,
                'datasource_index'     => $dsIndex,
                'datasource_name'      => $ds->getAlias(),
                'datasource_name_rrd'  => $ds->getName(),
                'datasource_type'      => $ds->getType(),
                'minimal_heartbeat'    => $ds->getHeartbeat(),
                'min_value'            => $ds->getMin(),
                'max_value'            => $ds->getMax(),
            ]));
        }

        // Only for deferred new CI, differs for missing DS
        $fileUuid = Uuid::uuid4();
        $fileUuidHex = $fileUuid->toString();
        $futures[] = async(fn () => $this->tables->setTableEntry('rrd_file', $fileUuidHex, ['uuid'], [
            'uuid'              => $fileUuidHex,
            // 'datanode_uuid'     => $this->nodeIdentifier, // ?!
            'metric_store_uuid' => $this->metricStoreIdentifier,
            'device_uuid'       => $ci->hostname, // ??
            'measurement_name'  => $ci->subject,
            'instance'          => $ci->instance,
            'tags'              => JsonString::encode($ci->tags),
            'filename'          => $info->getFilename(),
            'rrd_step'          => $info->getStep(),
            'rrd_version'       => $info->getRrdVersion(),
            'rrd_header_size'   => $info->getHeaderSize(),
            'rrd_datasource_list_checksum' => $dsListUuidHex, // TODO: uuid
            'rrd_archive_set_checksum'     => $rraSetUuidHex, // TODO: uuid
        ]));

        $ciConfig = new CiConfig(
            $fileUuid,
            $info->getFilename(),
            $info->getDsList()->listNames(),
            $info->getDsList()->getAliasesMap()
        );
        // War: processDeferredCi war rescheduleDeferredCi
        $futures[] = async(fn () => $this->lua->runScript('processDeferredCi', [
            JsonString::encode($ci)
        ], [
            JsonString::encode($ciConfig)
        ]));
        try {
            await($futures);
        } catch (\Throwable $e) {
            $this->logger->error($e->getMessage());
        }

        if (! isset($this->pendingCi[$ciKey])) {
            $this->logger->error('Failed to find CI (BUG!): ' . $ciKey);
        }
        unset($this->pendingCi[$ciKey]);
        EventLoop::queue($this->processDeferredMeasurement(...));
    }

    protected static function getCiLogName(Ci $ci, $ciKey): string
    {
        return JsonString::encode($ci) . " ($ciKey)";
    }
}
