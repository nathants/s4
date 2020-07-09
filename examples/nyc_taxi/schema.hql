
-- note: only the first 5 columns are consistent across the dataset, so we just use those

-- temp csv table to populate orc
CREATE EXTERNAL TABLE IF NOT EXISTS `yellow` (
  `vendor_id`        string,
  `pickup_datetime`  timestamp,
  `dropoff_datetime` timestamp,
  `passenger_count`  integer,
  `trip_distance`    double,
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/yellow/'
tblproperties("skip.header.line.count"="1");

-- orc table to query from
CREATE EXTERNAL TABLE IF NOT EXISTS `yellow_orc` (
  `vendor_id`        string,
  `pickup_datetime`  timestamp,
  `dropoff_datetime` timestamp,
  `passenger_count`  integer,
  `trip_distance`    double,
)
STORED AS ORC
LOCATION '/yellow_orc/'
tblproperties("orc.compress"="ZLIB");
