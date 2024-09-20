<?php

namespace IMEdge\MetricsFeature\Rrd;

use IMEdge\RrdStructure\NumericValue;
use Psr\Log\LoggerInterface;

use function Amp\Future\awaitAll;

class RrdSummary
{
    protected array $aggregateMethods = [
        'max'    => ['MAX', 'MAXIMUM'],
        'min'    => ['MIN', 'MINIMUM'],
        'avg'    => ['AVERAGE', 'AVERAGE'],
        'maxavg' => ['AVERAGE', 'MAXIMUM'],
        'stdev'  => ['AVERAGE', 'STDEV'],
    ];

    public function __construct(
        protected readonly RrdtoolRunner $rrd
    ) {
    }

    public function getPatterns(): array
    {
        $pattern = [];
        foreach ($this->aggregateMethods as $alias => $funcs) {
            $pattern[$alias] = ' DEF:%3$sa=%1$s:%2$s:' . $funcs[0]// . '3$sa' // Das letzte muss weg?
                . ' VDEF:%3$saa=%3$sa,' . $funcs[1]
                . ' PRINT:%3$saa:"%4$d %5$s %%.4lf"';
        }

        return $pattern;
    }

    /**
     * @param array $dataSources
     * @param array<string, array{0: int, 1: int} $periods name => [start, end] -> TODO: move TimeRange to RrdStructure
     * @return array
     * @throws \Exception
     */
    public function summariesForDataSources(array $dataSources, array $periods, LoggerInterface $logger): array
    {
        $pattern = $this->getPatterns();
        $commands = [];

        foreach ($periods as $period) {
            $baseCmd = 'graph /dev/null -f "" --start ' . $period[0] . ' --end ' . $period[1];
            $cmd = $baseCmd;

            foreach ($dataSources as $idx => $ds) {
                foreach ($pattern as $name => $rpn) {
                    $prefix = $name . $idx;
                    $cmd .= sprintf(
                        $rpn,
                        $this->string($ds->filename),
                        $this->string($ds->datasource),
                        $prefix,
                        $idx,
                        $name
                    );
                }
            }
            $commands[] = $cmd;
        }
        $futures = array_map($this->rrd->sendAsync(...), $commands);

        $result = awaitAll($futures);
        foreach ($result[0] as $e) {
            throw $e;
        }
        $res = [];
        foreach ($result[1] as $key => $stdout) {
            if ($stdout === false) {
                throw new \RuntimeException(sprintf("RrdSummary failed for: %s", $commands[$key]));
            }
            $this->processResult($dataSources, $stdout, $commands[$key], $res);
        }

        return $res;
    }

    protected function processResult(array $dataSources, string $stdout, string $cmd, array &$results): void
    {
        foreach (preg_split('/\n/', $stdout, -1, PREG_SPLIT_NO_EMPTY) as $line) {
            if (count(explode(' ', $line)) < 3) {
                throw new \RuntimeException('Unexpected line in RrdSummary: ' . $line);
            }
            list($dsId, $what, $value) = explode(' ', $line, 3);
            if (!is_numeric($dsId)) {
                // TODO: Should we fail here?
                echo $line . "<- RRD SUMMARY?!\n";
                continue;
            }

            $filename = $dataSources[$dsId]->filename;
            $dsName = $dataSources[$dsId]->datasource;
            if (! isset($results[$filename][$dsName])) {
                $results[$filename][$dsName] = [];
            }

            $results[$filename][$dsName][$what] = NumericValue::parseLocalizedFloat($value);
        }
    }

    protected function string(string $string): string
    {
        // TODO: Check and fix
        return "'" . addcslashes($string, "':") . "'";
    }
}
