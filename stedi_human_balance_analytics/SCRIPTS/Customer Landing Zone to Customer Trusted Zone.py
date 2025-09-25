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

# Script generated for node Customer Landing 
CustomerLanding_node1758777134908 = glueContext.create_dynamic_frame.from_catalog(database="final_project_stedi_human_balance", table_name="customer_landing", transformation_ctx="CustomerLanding_node1758777134908")

# Script generated for node SQL Query to filter
SqlQuery3511 = '''
SELECT * FROM customer_landing WHERE shareWithResearchAsOfDate IS NOT NULL;

'''
SQLQuerytofilter_node1758777138997 = sparkSqlQuery(glueContext, query = SqlQuery3511, mapping = {"customer_landing":CustomerLanding_node1758777134908}, transformation_ctx = "SQLQuerytofilter_node1758777138997")

# Script generated for node Customer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuerytofilter_node1758777138997, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758777076938", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrusted_node1758777143651 = glueContext.getSink(path="s3://final-project-stedi-human-balance-analytics/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1758777143651")
CustomerTrusted_node1758777143651.setCatalogInfo(catalogDatabase="final_project_stedi_human_balance",catalogTableName="customer _trusted")
CustomerTrusted_node1758777143651.setFormat("json")
CustomerTrusted_node1758777143651.writeFrame(SQLQuerytofilter_node1758777138997)
job.commit()