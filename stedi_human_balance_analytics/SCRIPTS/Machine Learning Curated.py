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
AccelerometerTrusted_node1758779732393 = glueContext.create_dynamic_frame.from_catalog(database="final_project_stedi_human_balance", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1758779732393")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1758779734182 = glueContext.create_dynamic_frame.from_catalog(database="final_project_stedi_human_balance", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1758779734182")

# Script generated for node SQL Query
SqlQuery3769 = '''
SELECT accelerometer_trusted.user,step_trainer_trusted.*,
accelerometer_trusted.x,accelerometer_trusted.y,
accelerometer_trusted.z
FROM step_trainer_trusted
INNER JOIN accelerometer_trusted
ON accelerometer_trusted.timeStamp = step_trainer_trusted.sensorReadingTime;
'''
SQLQuery_node1758779737371 = sparkSqlQuery(glueContext, query = SqlQuery3769, mapping = {"step_trainer_trusted":StepTrainerTrusted_node1758779734182, "accelerometer_trusted":AccelerometerTrusted_node1758779732393}, transformation_ctx = "SQLQuery_node1758779737371")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758779737371, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758779683131", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1758779784719 = glueContext.getSink(path="s3://final-project-stedi-human-balance-analytics/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1758779784719")
MachineLearningCurated_node1758779784719.setCatalogInfo(catalogDatabase="final_project_stedi_human_balance",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1758779784719.setFormat("json")
MachineLearningCurated_node1758779784719.writeFrame(SQLQuery_node1758779737371)
job.commit()