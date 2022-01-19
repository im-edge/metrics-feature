#!/usr/bin/php
<?php

use gipfl\Log\Logger;
use gipfl\Log\Writer\JournaldLogger;
use gipfl\Log\Writer\SystemdStdoutWriter;
use gipfl\Log\Writer\WritableStreamWriter;
use gipfl\SystemD\systemd;
use React\EventLoop\Loop;
use React\Stream\WritableResourceStream;

$logger = new Logger();
if (systemd::startedThisProcess()) {
    if (@file_exists(JournaldLogger::JOURNALD_SOCKET)) {
        $logger->addWriter((new JournaldLogger())->setIdentifier('icinga-metrics-shipper'));
    } else {
        $logger->addWriter(new SystemdStdoutWriter(Loop::get()));
    }
} else {
    $logger->addWriter(new WritableStreamWriter(new WritableResourceStream(STDERR, Loop::get())));
}
return $logger;
