<?php
return include dirname(dirname(__DIR__)) . '/icinga-graphing-config.php';
return (object) [
    'icinga' => (object) [
        'host' => '127.0.0.1',
        'port' => 5665,
        'user' => 'root',
        'pass' => '***',
    ],
    'redis' => (object) [
        'socket' => '/var/lib/icingagraphing/rrd/redis/redis.sock',
    ],
    'rrdcached' => (object) [
        'socket' => '/var/lib/icingagraphing/rrd/rrdcached.sock',
    ]
];
