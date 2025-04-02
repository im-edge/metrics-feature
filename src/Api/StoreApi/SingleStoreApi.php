<?php

namespace IMEdge\MetricsFeature\Api\StoreApi;

use Amp\Redis\RedisClient;
use IMEdge\Json\JsonString;
use IMEdge\Metrics\Ci;
use IMEdge\RedisTables\RedisTables;
use IMEdge\RedisUtils\LuaScriptRunner;
use IMEdge\RedisUtils\RedisResult;
use IMEdge\RpcApi\ApiMethod;
use IMEdge\RpcApi\ApiNamespace;
use IMEdge\RrdCached\RrdCachedClient;
use InvalidArgumentException;
use Psr\Log\LoggerInterface;
use Ramsey\Uuid\Uuid;
use Revolt\EventLoop;
use stdClass;

#[ApiNamespace('metricStore')]
class SingleStoreApi
{
    protected const PREFIX = 'metrics:';
    protected const HASH_SCHEDULED = 'scheduled-for-deletion';
    protected ?string $deletionFuture = null;
    protected LuaScriptRunner $lua;

    public function __construct(
        protected RedisClient $redis,
        protected RedisTables $redisTables,
        protected RrdCachedClient $client,
        protected string $rrdDataDirectory,
        protected LoggerInterface $logger,
    ) {
        $this->logger->notice('SINGLE STORE API is ready');
        $this->lua = new LuaScriptRunner($redis, dirname(__DIR__, 3) . '/lua', $this->logger);
        // $this->triggerDeletion();
    }

    #[ApiMethod]
    public function scheduleForDeletion(array $rrdFiles): bool
    {
        if (empty($rrdFiles)) {
            return false;
        }

        $args = [self::PREFIX . self::HASH_SCHEDULED];
        foreach ($rrdFiles as $file) {
            if (! is_object($file)) {
                throw new InvalidArgumentException('Valid file objects are required');
            }
            if (! isset($file->uuid)) {
                throw new InvalidArgumentException('File object has no uuid');
            }
            $args[] = $file->filename;
            $args[] = Uuid::fromString($file->uuid) . JsonString::encode(self::fileToCiKey($file));
        }
        $this->redis->execute('HMSET', ...$args);
        $this->deletionFuture ??= EventLoop::delay(0.1, $this->triggerDeletion(...));

        return true;
    }

    protected function triggerDeletion(): void
    {
        $files = $this->fetchFilesScheduledForDeletion();
        if (count($files) > 0) {
            try {
                foreach ($files as $filename => $ciKey) {
                    $this->deleteFile($filename, $ciKey);
                }
                $this->deletionFuture = EventLoop::delay(0.1, $this->triggerDeletion(...));
            } catch (\Throwable $e) {
                $this->logger->error('Failed to delete, will stop trying: ' . $e->getMessage());
            }
        } else {
            $this->deletionFuture = null;
        }
    }

    protected function fetchFilesScheduledForDeletion(): array
    {
        return RedisResult::toArray($this->redis->execute('HGETALL', self::PREFIX . self::HASH_SCHEDULED));
    }

    protected function deleteFile(string $filename, string $ciKey)
    {
        // $fileUuidHex = Uuid::fromString(preg_replace('/\.rrd$/', '', basename($filename)))->toString();
        // Hint: we have a bug, file UUID doesn't match filename
        $fileUuidHex = substr($ciKey, 0, 36);
        $ciKey = substr($ciKey, 36);
        $this->logger->notice("DELETE: $fileUuidHex -> $filename $ciKey");
        $this->lua->runScript('deferCi', [$ciKey], ['Scheduled for deletion']);
        try {
            $this->client->flushAndForget($filename);
        } catch (\Exception $e) {
            if (! preg_match('/no such file/i', $e->getMessage())) {
                $this->logger->warning("RrdCachedClient::flushAndForget($filename) failed: " . $e->getMessage());
            }
        }
        $file = $this->rrdDataDirectory . "/$filename";
        if (file_exists($file)) {
            if (!@unlink($file)) {
                $this->logger->error("Failed to delete $file");
            }
        } else {
            $this->logger->warning("Not deleting $file ($filename), it does not exist");
        }
        $this->lua->runScript('deleteCi', [$ciKey]);
        // TODO: Key is not correct. -> storeUuid and file? But... must also vanish from remote redis
        $this->redisTables->deleteTableEntry('rrd_file', $fileUuidHex, ['uuid']);
        $this->redis->execute('HDEL', self::PREFIX . self::HASH_SCHEDULED, $filename);
    }

    #[ApiMethod]
    public function getFiledScheduledForDeletion(): array
    {
        $result = [];
        $scheduled = RedisResult::toArray($this->redis->execute('HGETALL', self::PREFIX . self::HASH_SCHEDULED));
        foreach ($scheduled as $filename => $info) {
            $result[] = self::unWrapFileWithUuid($filename, $info);
        }

        return $result;
    }

    protected static function fileToCiKey(stdClass $file): Ci
    {
        // TODO: document uuid -> filename relation
        // "{\"uuid\":\"d773f409-f585-41b8-86ac-8243c058a41f\",\"device_uuid\":\"ad5fe1a1-d3ee-4e89-a2be-5db0947724c5\",\"filename\":\"09/099f714108a04bd9b3d455afa10d05b2.rrd\",\"measurement_name\":\"if_traffic\",\"instance\":\"9\",\"tags\":[]}"
        // -> "[\"81c6d7be-4eab-4ff6-967e-c8ad2f02d340\",\"SnmpScenario\",\"interfacePacket\",[]]"

        return new Ci($file->device_uuid, $file->measurement_name, $file->instance, $file->tags ?? []);
    }

    protected static function unWrapFileWithUuid(string $filename, string $uuidWithCiString)
    {
        $uuid = substr($uuidWithCiString, 0, 36);
        $ci = Ci::fromSerialization(JsonString::decode(substr($uuidWithCiString, 36)));
        return (object) [
            'uuid' => $uuid,
            'filename' => $filename,
            'device_uuid' => $ci->hostname,
            'measurement_name' => $ci->subject,
            'instance' => $ci->instance,
            'tags' => $ci->tags,
        ];
    }
}
