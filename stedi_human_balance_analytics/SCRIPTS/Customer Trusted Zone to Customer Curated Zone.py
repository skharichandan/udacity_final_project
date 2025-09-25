import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1758778502021 = glueContext.create_dynamic_frame.from_catalog(database="final_project_stedi_human_balance", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1758778502021")

# Script generated for node Customer Trusted
CustomerTrusted_node1758778503396 = glueContext.create_dynamic_frame.from_catalog(database="final_project_stedi_human_balance", table_name="customer _trusted", transformation_ctx="CustomerTrusted_node1758778503396")

# Script generated for node SQL Query to include customer who have acclerometer data
SqlQuery3564 = '''
SELECT DISTINCT customer_trusted.*
FROM accelerometer_trusted
INNER JOIN customer_trusted
ON accelerometer_trusted.user = customer_trusted.email;

'''
SQLQuerytoincludecustomerwhohaveacclerometerdata_node1758778506697 = sparkSqlQuery(glueContext, query = SqlQuery3564, mapping = {"customer_trusted":CustomerTrusted_node1758778503396, "accelerometer_trusted":AccelerometerTrusted_node1758778502021}, transformation_ctx = "SQLQuerytoincludecustomerwhohaveacclerometerdata_node1758778506697")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=SQLQuerytoincludecustomerwhohaveacclerometerdata_node1758778506697, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758778446195", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1758778509911 = glueContext.getSink(path="s3://final-project-stedi-human-balance-analytics/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1758778509911")
CustomerCurated_node1758778509911.setCatalogInfo(catalogDatabase="final_project_stedi_human_balance",catalogTableName="customer_curated")
CustomerCurated_node1758778509911.setFormat("json")
CustomerCurated_node1758778509911.writeFrame(SQLQuerytoincludecustomerwhohaveacclerometerdata_node1758778506697)
job.commit()