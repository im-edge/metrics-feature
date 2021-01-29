<?php

namespace iPerGraph;

use InvalidArgumentException;
use JsonSerializable;
use function is_float;
use function is_int;
use function json_decode;
use function json_encode;
use function time;
use const JSON_PRESERVE_ZERO_FRACTION;
use const JSON_UNESCAPED_SLASHES;
use const JSON_UNESCAPED_UNICODE;

class PerfData implements JsonSerializable
{
    const JSON_FLAGS = JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRESERVE_ZERO_FRACTION;

    protected $timestamp;

    protected $ci;

    protected $values;

    public function __construct($ci, $values, $timestamp = null)
    {
        $this->ci = $ci;
        $this->values = $values;
        $this->timestamp = $timestamp;
    }

    public function jsonSerialize()
    {
        return [
            'ts' => $this->timestamp,
            'ci' => $this->ci,
            'dp' => $this->values,
        ];
    }

    public function getTime()
    {
        if ($this->timestamp === null) {
            return time();
        }
        if (is_int($this->timestamp)) {
            return $this->timestamp;
        }
        if (is_float($this->timestamp)) {
            return (int) $this->timestamp;
        }

        throw new InvalidArgumentException('Timestamp expected, got: ' . var_export($this->timestamp, 1));
    }

    public function getValues()
    {
        return $this->values;
    }

    public function toJson()
    {
        return json_encode($this, self::JSON_FLAGS);
    }

    public static function fromJson($string)
    {
        // TODO: Fail, use JSON class
        $obj = json_decode($string, false);
        return new static($obj->ci, $obj->dp, $obj->ts);
    }
}
