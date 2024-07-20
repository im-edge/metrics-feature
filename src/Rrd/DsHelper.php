<?php

namespace IMEdge\MetricsFeature\Rrd;

use IMEdge\Metrics\Measurement;
use IMEdge\Metrics\MetricDatatype;
use IMEdge\RrdStructure\Ds;
use IMEdge\RrdStructure\DsList;
use Psr\Log\LoggerInterface;

use function mb_substr;
use function preg_match;
use function preg_replace;

class DsHelper
{
    public static function getDataSourcesForMeasurement(
        LoggerInterface $logger,
        Measurement $measurement,
        ?DsList $formerDsList = null
    ): DsList {
        if ($formerDsList) {
            $map = $formerDsList->getAliasesMap();
            $result = clone($formerDsList);
        } else {
            $map = [];
            $result = new DsList();
        }

        foreach ($measurement->getMetrics() as $metric) {
            if ($result->hasAlias($metric->label)) {
                continue;
            }
            $isCounter = $metric->type === MetricDatatype::COUNTER;

            $ds = new Ds(
                self::getDsNameFor($logger, $metric->label, $map),
                $isCounter ? MetricDatatype::DERIVE->value : $metric->type->value,
                8640,
                ($isCounter || $metric->type === MetricDatatype::DDERIVE) ? 0 : null
            );
            $ds->setAlias($metric->label);
            $result->add($ds);
        }

        return $result;
    }

    protected static function getDsNameFor(LoggerInterface $logger, string $label, &$seen = [])
    {
        $name = self::shorten(self::replaceInvalidCharacters($label));
        while (isset($seen[$name])) {
            $logger->debug(sprintf("Alias '%s' already exists", $name));
            if (preg_match('/_(\d+)$/', $name, $numMatch)) {
                $name = preg_replace('/_\d+$/', '_' . ((int) $numMatch[1] + 1), $name);
            } else {
                $name = self::shorten($name, 17) . '_2';
            }
            $logger->debug(sprintf("New alias: '%s'", $name));
        }
        if ($name !== $label) {
            $logger->debug(sprintf("Aliased DS '%s' to '%s'", $label, $name));
        }
        $seen[$name] = $label;

        return $name;
    }

    protected static function replaceInvalidCharacters(string $name): string
    {
        return preg_replace('/_+/', '_', preg_replace('/\W/', '_', $name));
    }

    protected static function shorten(string $string, int $length = 19): string
    {
        return mb_substr($string, 0, $length);
    }
}
