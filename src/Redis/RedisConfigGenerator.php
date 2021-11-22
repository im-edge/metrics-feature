<?php

namespace IcingaMetrics\Redis;

class RedisConfigGenerator
{
    public static function forPath($path) : string
    {
        return 'daemonize no
port 0
#appendonly yes
unixsocket ' . $path . '/redis.sock
unixsocketperm 770
timeout 0
loglevel notice
logfile ""
databases 16

save 900 1
save 300 10
save 60 10000
# TODO: change this and monitor redis
stop-writes-on-bgsave-error yes
rdbcompression yes
rdbchecksum yes
dbfilename dump.rdb
dir ' . $path . '
maxmemory 2048mb
maxmemory-policy noeviction
lua-time-limit 10000
slowlog-log-slower-than 5000
slowlog-max-len 128
latency-monitor-threshold 0
notify-keyspace-events ""

############################### ADVANCED CONFIG ###############################
hash-max-ziplist-entries 512
hash-max-ziplist-value 64
list-max-ziplist-entries 512
list-max-ziplist-value 64
set-max-intset-entries 512
zset-max-ziplist-entries 128
zset-max-ziplist-value 64
hll-sparse-max-bytes 3000
activerehashing yes
client-output-buffer-limit normal 0 0 0
client-output-buffer-limit slave 256mb 64mb 60
client-output-buffer-limit pubsub 32mb 8mb 60
hz 10
';
    }
}
