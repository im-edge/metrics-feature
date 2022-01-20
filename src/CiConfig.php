<?php

namespace IcingaMetrics;

use JsonSerializable;
use Ramsey\Uuid\UuidInterface;
use function bin2hex;
use function substr;

class CiConfig implements JsonSerializable
{
    public UuidInterface $uuid;

    public string $filename;

    /**
     * Defines DataSource order
     *
     * Cannot be defined by the map, as other languages have no sorted dictionaries
     */
    public array $dsNames;

    /**
     * Maps Datasource names to their 19 character RRD counterpart
     */
    public array $dsMap;

    public static function create(UuidInterface $uuid, array $dsNames, array $dsMap): self
    {
        $self = new static();
        $self->uuid = $uuid;
        $hex = bin2hex($uuid->getBytes());
        $self->filename = substr($hex, 0, 2) . '/' . substr($hex, 2, 2) . "/$hex.rrd";
        $self->dsNames = $dsNames;
        $self->dsMap = $dsMap;
        return $self;
    }

    public function jsonSerialize(): object
    {
        return (object) [
            'uuid'     => $this->uuid->toString(),
            'filename' => $this->filename,
            'dsNames'  => $this->dsNames,
            'dsMap'    => $this->dsMap,
        ];
    }
}
