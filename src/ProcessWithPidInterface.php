<?php

namespace IcingaMetrics;

interface ProcessWithPidInterface
{
    public function getProcessPid(): ?int;
}
