Deferred, Ablauf
================

* read from stream
* RrdStore->





pushMeasurements([
   "jsonstring",
   "jsonstring"
])


Why a JSON string? -> we cannot push nested structured data

"jsonstring" = [
  "cistring", (also JSON, will not be decoded by lua/redis)
  timestamp,
  "metrics" -- used to be datapoint list
]
"cistring" => [hostname, service, instance]
"metrics" => [["inOctets", "12312123", "COUNTER"], []]




rrd_archive
rrd_archive_set_checksum VARBINARY(16) NOT NULL,
rra_index
consolidation_function
row_count
settings
PRIMARY KEY (rrd_archive_set_checksum, rra_index),
CONSTRAINT rrd_archive_set_checksum
FOREIGN KEY rrd_archive_set (rrd_archive_set_checksum)
REFERENCES rrd_archive_set (checksum)
ON DELETE CASCADE
ON UPDATE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_bin;
