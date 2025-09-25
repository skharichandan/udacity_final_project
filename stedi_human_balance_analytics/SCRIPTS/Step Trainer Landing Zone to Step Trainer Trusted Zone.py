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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1758779173364 = glueContext.create_dynamic_frame.from_catalog(database="final_project_stedi_human_balance", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1758779173364")

# Script generated for node Customer Curated
CustomerCurated_node1758779172396 = glueContext.create_dynamic_frame.from_catalog(database="final_project_stedi_human_balance", table_name="customer_curated", transformation_ctx="CustomerCurated_node1758779172396")

# Script generated for node SQL Query
SqlQuery3516 = '''
SELECT step_trainer_landing.*
FROM step_trainer_landing
INNER JOIN customer_curated
ON customer_curated.serialNumber = step_trainer_landing.serialNumber;

'''
SQLQuery_node1758779176852 = sparkSqlQuery(glueContext, query = SqlQuery3516, mapping = {"step_trainer_landing":StepTrainerLanding_node1758779173364, "customer_curated":CustomerCurated_node1758779172396}, transformation_ctx = "SQLQuery_node1758779176852")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758779176852, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758779080607", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1758779180052 = glueContext.getSink(path="s3://final-project-stedi-human-balance-analytics/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1758779180052")
StepTrainerTrusted_node1758779180052.setCatalogInfo(catalogDatabase="final_project_stedi_human_balance",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1758779180052.setFormat("json")
StepTrainerTrusted_node1758779180052.writeFrame(SQLQuery_node1758779176852)
job.commit()