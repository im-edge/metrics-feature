<?php

namespace IcingaGraphing;

class InstanceConfig
{
    protected $basedir;

    public function __construct(string $basedir)
    {
        $this->basedir = $basedir;
    }

    public function getBaseDir() : string
    {
        return $this->basedir;
    }

    public function getRedisSocketUri() : string
    {
        return 'redis+unix://' . $this->getBaseDir() . '/redis/redis.sock';
    }

    public function getRrdFilesDir() : string
    {
        return $this->getBaseDir() . '/redis/redis.sock';
    }

    public function getRrdCachedSocket() : string
    {
        return $this->getBaseDir() . '/redis/redis.sock';
    }
}
