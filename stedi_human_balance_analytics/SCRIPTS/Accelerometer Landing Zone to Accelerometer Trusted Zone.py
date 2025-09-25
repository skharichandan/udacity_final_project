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

# Script generated for node Customer Trusted
CustomerTrusted_node1758777812221 = glueContext.create_dynamic_frame.from_catalog(database="final_project_stedi_human_balance", table_name="customer _trusted", transformation_ctx="CustomerTrusted_node1758777812221")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1758777819795 = glueContext.create_dynamic_frame.from_catalog(database="final_project_stedi_human_balance", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1758777819795")

# Script generated for node SQL Query to join 
SqlQuery3575 = '''
SELECT DISTINCT accelerometer_landing.*
FROM accelerometer_landing
INNER JOIN customer_trusted
ON accelerometer_landing.user = customer_trusted.email;

'''
SQLQuerytojoin_node1758777824884 = sparkSqlQuery(glueContext, query = SqlQuery3575, mapping = {"accelerometer_landing":AccelerometerLanding_node1758777819795, "customer_trusted":CustomerTrusted_node1758777812221}, transformation_ctx = "SQLQuerytojoin_node1758777824884")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuerytojoin_node1758777824884, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758777773110", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1758777829073 = glueContext.getSink(path="s3://final-project-stedi-human-balance-analytics/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1758777829073")
AccelerometerTrusted_node1758777829073.setCatalogInfo(catalogDatabase="final_project_stedi_human_balance",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1758777829073.setFormat("json")
AccelerometerTrusted_node1758777829073.writeFrame(SQLQuerytojoin_node1758777824884)
job.commit()