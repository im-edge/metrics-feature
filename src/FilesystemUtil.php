<?php

namespace IcingaMetrics;

use RuntimeException;

abstract class FilesystemUtil
{
    public static function requireDirectory(string $directory, bool $recursive = false, int $mode = 0755)
    {
        @mkdir($directory, $mode, $recursive);
        if (! @is_dir($directory) || ! @is_writable($directory)) {
            throw new RuntimeException("Unable do create '$directory'");
        }
    }
}
