CREATE EXTERNAL TABLE `step_trainer_trusted`(
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://final-project-stedi-human-balance-analytics/step_trainer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='Step Trainer Landing Zone to Step Trainer Trusted Zone', 
  'CreatedByJobRun'='jr_297e88f3c265c93890334e75a7f12ac293a28582351d5d0a71c421023ce21be5', 
  'classification'='json')
