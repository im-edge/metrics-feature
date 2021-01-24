<?php

namespace iPerGraph;

use JsonSerializable;
use function json_encode;
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

    public function toJson()
    {
        return json_encode($this, self::JSON_FLAGS);
    }
}
