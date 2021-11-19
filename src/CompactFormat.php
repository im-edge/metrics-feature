<?php

namespace IcingaMetrics;

use function abs;
use function floor;
use function log;
use function sprintf;

class CompactFormat
{
    const BINARY = 0;
    const DECIMAL  = 1;

    const BASE = [
        self::BINARY => 1024, // Binary
        self::DECIMAL  => 1000, // Decimal
    ];

    const PREFIX_SYMBOL = [
        self::BINARY  => ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi'],
        self::DECIMAL => [
            -4 => 'p',
            -3 => 'n',
            -2 => 'µ',
            -1 => 'm',
            0 => '',
            1 => 'k',
            2 => 'M',
            3 => 'G',
            4 => 'T',
            5 => 'P',
            6 => 'E',
            7 => 'Z',
            8 => 'Y',
        ],
    ];

    const PREFIX_NAME = [
        self::BINARY  => ['', 'kibi', 'mebi', 'gibi', 'tebi', 'pebi', 'exbi', 'zebi', 'yobi'],
        self::DECIMAL => [
            -4 => 'pico',
            -3 => 'nano',
            -2 => 'micro',
            -1 => 'milli',
            0 => '',
            1 => 'kilo',
            2 => 'mega',
            3 => 'giga',
            4 => 'tera',
            5 => 'peta',
            6 => 'exa',
            7 => 'zeta',
            8 => 'yotta'
        ],
    ];

    public static function decimal($value)
    {
        return self::compact($value, $standard = self::DECIMAL);
    }

    public static function bits($value, $standard = self::DECIMAL)
    {
        return self::compact($value, $standard) . 'bit';
    }

    public static function bitsPerSecond($value, $standard = self::DECIMAL)
    {
        return self::compact($value, $standard) . 'bit/s';
    }

    public static function bytes($value, $standard = self::BINARY)
    {
        return self::compact($value, $standard) . 'B';
    }

    public static function hz($value, $standard = self::BINARY)
    {
        return self::compact($value, $standard) . 'hz';
    }

    public static function compact($value, $standard)
    {
        $base = self::BASE[$standard];
        $symbols = self::PREFIX_SYMBOL[$standard];
        if ($value === 0 || $value === 0.0) {
            return sprintf('%.3G %s', 0, $symbols[0]);
        }

        if ($value < 0) {
            $value = abs($value);
            $sign = '-';
        } else {
            $sign = '';
        }

        $exponent = floor(log($value, $base));
        $result = $value / ($base ** $exponent);
        if ($exponent < 0) {
            if ($standard === self::BINARY) {
                $result = 0;
                $exponent = 0;
            } elseif (round($result) >= $base) {
                $result /= $base;
                $exponent--;
            }
        } elseif (round($result) >= $base) {
            $result /= $base;
            $exponent++;
        }

        return sprintf('%s%.3G %s', $sign, $result, $symbols[$exponent]);
    }
}
