<?php

namespace IMEdge\MetricsFeature;

use IMEdge\Json\JsonSerialization;
use IMEdge\RrdStructure\DsList;
use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidInterface;

use function bin2hex;
use function substr;

class CiConfig implements JsonSerialization
{
    public function __construct(
        public readonly UuidInterface $uuid,
        public readonly string $filename,
        /**
         * Defines DataSource order
         *
         * Cannot be defined by the map, as other languages have no sorted dictionaries
         * @var string[] $dsNames
         */
        public readonly array $dsNames,

        /**
         * Maps Datasource names to their 19 character RRD counterpart
         *
         * @var array<string, string>
         */
        public readonly array $dsMap,
    ) {
    }

    public static function forDsList(DsList $dsList): CiConfig
    {
        $uuid = Uuid::uuid4();
        return new CiConfig(
            $uuid,
            self::filenameForUuid($uuid),
            $dsList->listNames(),
            $dsList->getAliasesMap()
        );
    }

    /**
     * @param array $dsNames
     * @param array $dsMap
     * @return CiConfig
     */
    public static function create(array $dsNames, array $dsMap): CiConfig
    {
        $uuid = Uuid::uuid4();
        return new CiConfig(
            $uuid,
            self::filenameForUuid($uuid),
            $dsNames,
            $dsMap
        );
    }

    protected static function filenameForUuid(UuidInterface $uuid): string
    {
        $hex = bin2hex($uuid->getBytes()); // Bullshit
        return substr($hex, 0, 2) . "/$hex.rrd";
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

    public static function fromSerialization($any): CiConfig
    {
        return new CiConfig(
            Uuid::fromString($any->uuid),
            $any->filename,
            $any->dsNames,
            $any->dsMap,
        );
    }
}
