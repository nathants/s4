-- note: only the first 5 columns are consistent across the taxi dataset, so we just use those

-- temp csv table to populate orc
CREATE EXTERNAL TABLE IF NOT EXISTS `taxi_csv` (
  `vendor`     string,
  `pickup`     timestamp,
  `dropoff`    timestamp,
  `passengers` integer,
  `distance`   double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/taxi_csv/'
tblproperties("skip.header.line.count"="1");

-- orc table to query from
CREATE EXTERNAL TABLE IF NOT EXISTS `taxi` (
  `vendor`     string,
  `pickup`     timestamp,
  `dropoff`    timestamp,
  `passengers` integer,
  `distance`   double
)
STORED AS ORC
LOCATION '/taxi/';
