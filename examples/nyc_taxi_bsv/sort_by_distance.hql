CREATE EXTERNAL TABLE `sorted` (
  `distance` double
)
STORED AS ORC
LOCATION '/sorted/';

TRUNCATE TABLE `sorted`;
