CREATE EXTERNAL TABLE `machine_learning_curated`(
  `user` string COMMENT 'from deserializer', 
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer', 
  `x` double COMMENT 'from deserializer', 
  `y` double COMMENT 'from deserializer', 
  `z` double COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://final-project-stedi-human-balance-analytics/machine_learning_curated/'
TBLPROPERTIES (
  'CreatedByJob'='Machine Learning Curated', 
  'CreatedByJobRun'='jr_45e25af8686f5dafaafdf53f6fd4a8c95a8a74568f072a4c09a1f0e3ddde3396', 
  'classification'='json')
