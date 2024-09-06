<?php

namespace IMEdge\MetricsFeature\FileInventory;

use IMEdge\RrdStructure\RraSet;

class SampleRraSet
{
    protected static array $defaultCreate = [ // -> step = 1 second
        // 3600 entries with 1 second step = 1 hours
        'RRA:AVERAGE:0.5:1:2880',
        // 2880 entries with 1 minute step = 48 hours
        'RRA:AVERAGE:0.5:60:2880',
        // 2880 entries with 5 minute step = 10 days
        'RRA:AVERAGE:0.5:5:2880',
        // 4320 entries with 30 minute step = 90 days
        'RRA:AVERAGE:0.5:30:4320',
        // 5840 entries with 360 minute step = 4 years
        'RRA:AVERAGE:0.5:360:5840',
        'RRA:MAX:0.5:1:2880',
        'RRA:MAX:0.5:5:2880',
        'RRA:MAX:0.5:30:4320',
        'RRA:MAX:0.5:360:5840',
        'RRA:MIN:0.5:1:2880',
        'RRA:MIN:0.5:5:2880',
        'RRA:MIN:0.5:30:4320',
        'RRA:MIN:0.5:360:5840'
    ];

    // PNP default RRA config
    // you will get 400kb of data per datasource
    protected static array $pnpDefault = [ // step = 60 seconds
        // 2880 entries with 1 minute step = 48 hours
        'RRA:AVERAGE:0.5:1:2880',
        // 2880 entries with 5 minute step = 10 days
        'RRA:AVERAGE:0.5:5:2880',
        // 4320 entries with 30 minute step = 90 days
        'RRA:AVERAGE:0.5:30:4320',
        // 5840 entries with 360 minute step = 4 years
        'RRA:AVERAGE:0.5:360:5840',
        'RRA:MAX:0.5:1:2880',
        'RRA:MAX:0.5:5:2880',
        'RRA:MAX:0.5:30:4320',
        'RRA:MAX:0.5:360:5840',
        'RRA:MIN:0.5:1:2880',
        'RRA:MIN:0.5:5:2880',
        'RRA:MIN:0.5:30:4320',
        'RRA:MIN:0.5:360:5840'
    ];

    protected static array $kickstartWithSeconds = [
        // 4 hours of one per second (346kB)
        // 'RRA:AVERAGE:0.5:1:14400',
        // 'RRA:MAX:0.5:1:14400',
        // 'RRA:MIN:0.5:1:14400',
        // 2 days of four per minute (1 every 15s) (+277kB)
        'RRA:AVERAGE:0.5:15:11520',
        'RRA:MAX:0.5:15:11520',
        'RRA:MIN:0.5:15:11520',
    ];

    protected static array $fullWithSeconds = [
        // 4 hours of one per second (346kB)
        'RRA:AVERAGE:0.5:1:14400',
        'RRA:MAX:0.5:1:14400',
        'RRA:MIN:0.5:1:14400',
        // 2 days of four per minute (1 every 15s) (+277kB)
        'RRA:AVERAGE:0.5:15:11520',
        'RRA:MAX:0.5:15:11520',
        'RRA:MIN:0.5:15:11520',
        // 2880 entries with 5 minute step = 10 days (+60kB)
        'RRA:AVERAGE:0.5:300:2880',
        'RRA:MAX:0.5:300:2880',
        'RRA:MAX:0.5:300:2880',
        // 4320 entries with 30 minute step = 90 days
        'RRA:AVERAGE:0.5:1800:4320',
        'RRA:MIN:0.5:1800:4320',
        'RRA:MAX:0.5:1800:4320',
        // 5840 entries with 360 minute step = 4 years
        'RRA:AVERAGE:0.5:21600:5840',
        'RRA:MIN:0.5:21600:5840',
        'RRA:MAX:0.5:21600:5840',
    ];

    // Faster RRA config
    protected static array $fasterCreate = [
        // 1800 entries with 1 second step = 30 minutes
        'RRA:AVERAGE:0.5:1:1800',
        // 2880 entries with 1 minute step = 48 hours
        'RRA:AVERAGE:0.5:60:2880',
        // 2880 entries with 5 minute step = 10 days
        'RRA:AVERAGE:0.5:300:2880',
        // 4320 entries with 30 minute step = 90 days
        'RRA:AVERAGE:0.5:1800:4320',
        // 5840 entries with 360 minute step = 4 years
        'RRA:AVERAGE:0.5:21600:5840',
        'RRA:MAX:0.5:60:2880',
        'RRA:MAX:0.5:300:2880',
        'RRA:MAX:0.5:1800:4320',
        'RRA:MAX:0.5:21600:5840',
        'RRA:MIN:0.5:60:2880',
        'RRA:MIN:0.5:300:2880',
        'RRA:MIN:0.5:1800:4320',
        'RRA:MIN:0.5:21600:5840'
    ];

    public static function faster(): RraSet
    {
        return new RraSet(static::$fasterCreate);
    }

    public static function pnpDefaults(): RraSet
    {
        return new RraSet(static::$defaultCreate);
    }

    public static function kickstartWithSeconds(): RraSet
    {
        return new RraSet(static::$kickstartWithSeconds);
    }
}
