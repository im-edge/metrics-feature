<?php

namespace IMEdge\MetricsFeature\Rrd;

use gipfl\Json\JsonSerialization;
use InvalidArgumentException;

/**
 * When a command is completed, RRDtool will print the string 'OK', followed by timing information of the form
 * u:usertime s:systemtime. Both values are the running totals of seconds since RRDtool was started
 */
class TimeStatistics implements JsonSerialization
{
    /**
     * Amount of CPU time spent in user-mode code (outside the kernel) within the process
     *
     * This is only actual CPU time used in executing the process. Other processes and time the process spends blocked
     * do not count towards this figure
     */
    public float $user;

    /**
     * Amount of CPU time spent in the kernel within the process
     *
     * This means executing CPU time spent in system calls within the kernel, as opposed to library code, which is
     * still running in user-space. Like 'user', this is only CPU time used by the process. See below for a brief
     * description of kernel mode (also known as 'supervisor' mode) and the system call mechanism.
     */
    public float $system;

    /**
     * Wall clock timek
     *
     * Time from start to finish of the call. This is all elapsed time including time slices used by other processes
     * and time the process spends blocked (for example if it is waiting for I/O to complete).
     */
    public float $real;

    public function __construct(float $user, float $system, float $real)
    {
        $this->user = $user;
        $this->system = $system;
        $this->real = $real;
    }

    public static function fromSerialization($any): TimeStatistics
    {
        return new static($any->user, $any->system, $any->float);
    }

    /**
     * Line saying OK u:1.14 s:0.07 r:1.21
     * This can be is localized:
     * OK u:0,02 s:0,00 r:0,01
     * OK u:0.02 s:0.00 r:0.01
     */
    public static function parse(string $line): ?TimeStatistics
    {
        if (preg_match('/^OK\su:([0-9.,]+)\ss:([0-9.,]+)\sr:([0-9.,]+)$/', $line, $m)) {
            return new TimeStatistics(
                static::parseLocalizedFloat($m[1]),
                static::parseLocalizedFloat($m[2]),
                static::parseLocalizedFloat($m[3])
            );
        }

        throw new InvalidArgumentException("Invalid rrdtool timings: $line");
    }

    // duplicate
    public static function parseLocalizedFloat($string): float
    {
        return (float) \str_replace(',', '.', $string);
    }

    public function jsonSerialize(): object
    {
        return (object) [
            'user'   => $this->user,
            'system' => $this->system,
            'real'   => $this->real,
        ];
    }
}
