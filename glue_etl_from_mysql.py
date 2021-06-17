import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "hour_elec_db", table_name = "hour_elec_hour_elec_table", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "hour_elec_db", table_name = "hour_elec_hour_elec_table", transformation_ctx = "datasource0")

# Convert to a dataframe and partition based on "partition_col"
partitioned_dataframe = datasource0.toDF().repartition(1)

# Convert back to a DynamicFrame for further processing.
partitioned_dynamicframe = DynamicFrame.fromDF(partitioned_dataframe, glueContext, "partitioned_df")

## @type: ApplyMapping
## @args: [mapping = [("datetime", "timestamp", "datetime", "timestamp"), ("elec_value", "double", "elec_value", "double")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = partitioned_dynamicframe, mappings = [("datetime", "date", "datetime", "date"), ("elec_value", "float", "elec_value", "float")], transformation_ctx = "applymapping1")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://sagemaker-cn-north-1-456370280007/sagemaker/goldwind/step-function"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
bucket_name = 'sagemaker-cn-north-1-456370280007' #改为自己的存储桶
base_key = 'sagemaker/goldwind/step-function/{}/'.format(datetime.datetime.now().strftime('%Y-%m-%d')) #改为自己的前缀，保留datetime部分
s3_data_path = "s3://{}/{}".format(bucket_name, base_key)
datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": s3_data_path}, format = "csv", transformation_ctx = "datasink2")

import boto3
client = boto3.client('s3')

response = client.list_objects(
    Bucket=bucket_name,
    Prefix=base_key,
)
name = response["Contents"][0]["Key"]

client.copy_object(Bucket=bucket_name, CopySource=bucket_name+"/"+name, Key=base_key+"hour_elec.csv")
client.delete_object(Bucket=bucket_name, Key=name)
job.commit()