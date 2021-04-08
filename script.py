import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, [ 'JOB_NAME', 'INPUT_LOC', 'OUTPUT_LOC' ])
input_location = args['INPUT_LOC']
output_location = args['OUTPUT_LOC']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

job.init(args['JOB_NAME'], args)

datasource0 = glueContext.create_dynamic_frame.from_options(
    connection_type = "s3",
    format = "json",
    connection_options = {
        "paths": [ input_location ]
    },
    format_options = {
        "withHeader": False,
        "separator": ","
    }
)

glueContext.write_dynamic_frame.from_options(
    frame = datasource0,
    connection_type = "s3",
    connection_options = {
        "path": output_location
    },
    format = "parquet"
)

job.commit()
