<?php

namespace IcingaGraphing\NamingStrategy;

use gipfl\RrdTool\Ds;
use gipfl\RrdTool\DsList;
use Psr\Log\LoggerInterface;

class DefaultNamingStrategy
{
    protected $logger;

    public function __construct(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    public function getFilename($ci)
    {
        $checksum = sha1($ci);

        return substr($checksum, 0, 2) . '/' . substr($checksum, 2, 2) . "/$checksum.rrd";
        // $ci = preg_replace('/[\0-\31<>:"/\\\|?*]/u', '_', $ci);
    }

    public function prepareCiConfig($ci, $keyValue)
    {
        $dsList = $this->getDataSourcesFor($keyValue);
        $map = array_combine(array_keys($keyValue), $dsList->listNames());
        $filename = $this->getFilename($ci);
        return (object) [
            'filename' => $filename,
            'dsNames'  => $dsList->listNames(),
            'dsMap'    => $map,
        ];
    }

    /**
     * @param array $parts { name => [ type, value ], otherName => ... }
     * @param DsList|null $formerDsList
     * @return DsList
     */
    public function getDataSourcesFor($parts, DsList $formerDsList = null)
    {
        $result = new DsList();
        if ($formerDsList) {
            $result = clone($formerDsList);
        }
        // TODO: Former mapping!!
        $seen = [];
        foreach ($parts as $originalName => list($type, $value)) {
            $result->add(Ds::create($this->getDsNameFor($originalName, $seen), $type, 8640));
        }

        return $result;
    }

    protected function getDsNameFor($label, &$seen = [])
    {
        $name = $this->shorten($this->replaceInvalidCharacters($label));
        while (isset($seen[$name])) {
            $this->logger->debug(sprintf("Alias '%s' already exists", $name));
            if (\preg_match('/_(\d+)$/', $name, $numMatch)) {
                $name = \preg_replace('/_\d+$/', '_' . ((int) $numMatch[1] + 1), $name);
            } else {
                $name = $this->shorten($name, 17) . '_2';
            }
            $this->logger->debug(sprintf("New alias: '%s'", $name));
        }
        if ($name !== $label) {
            $this->logger->debug(sprintf("Aliased DS '%s' to '%s'", $label, $name));
        }
        $seen[$name] = true;

        return $name;
    }

    protected function replaceInvalidCharacters($name)
    {
        return preg_replace('/_+/', '_', preg_replace('/\W/', '_', $name));
    }

    protected function shorten($string, $length = 19)
    {
        return \mb_substr($string, 0, $length);
    }
}
