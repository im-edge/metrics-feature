<?php

namespace IcingaMetrics;

use gipfl\IcingaPerfData\Ci;
use gipfl\IcingaPerfData\Measurement;
use gipfl\Json\JsonString;
use InvalidArgumentException;
use JsonSerializable;
use function is_float;
use function is_int;
use function time;

class PerfData implements JsonSerializable
{
    protected Ci $ci;
    /** @var int|float|null */
    protected $timestamp;
    protected array $values;

    public function __construct(Ci $ci, array $values, $timestamp = null)
    {
        $this->ci = $ci;
        $this->values = $values;
        $this->timestamp = $timestamp;
    }

    public function jsonSerialize(): object
    {
        return (object) [
            'ts' => $this->timestamp,
            'ci' => JsonString::encode($this->ci), // Encoded twice -> LUA
            'dp' => $this->values,
        ];
    }

    public static function fromMeasurement(Measurement $measurement): PerfData
    {
        $values = [];
        foreach ($measurement->getMetrics() as $metric) {
            if ($metric->isCounter()) {
                $values[$metric->getLabel()] = ((string) $metric->getValue()) . 'c';
            } else {
                $values[$metric->getLabel()] = $metric->getValue();
            }
        }

        return new static($measurement->getCi(), $values, $measurement->getTimestamp() ?? time());
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

    public function getCi(): Ci
    {
        return $this->ci;
    }

    public function getValues(): array
    {
        return $this->values;
    }
}
