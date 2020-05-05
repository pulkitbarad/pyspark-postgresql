from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import requests
import os
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def main():
    spark_session = get_spark_session("Test_application")
    
    file_metadata = json.load(open("/artifacts/metadata/vehicle.json"))
    vehicle_metadata = file_metadata["vehicle"]
    load_temp_table(spark_session,vehicle_metadata)
    
    result_dataframe = spark_session.sql("""
        SELECT
            *,
            current_timestamp() AS CREATED_AT,
            "SOURCE_LOAD_PIPELINE_VEHICLE" AS CREATED_BY
        FROM
            Source_Vehicle
    """)
    print_log(result_dataframe.take(1))


def print_log(statement):
    print( f'{datetime.now().strftime("%y/%m/%d %H:%M:%S")} ======CUSTOM====== {statement}')


def get_spark_session(application_name):
    spark_session = \
        SparkSession.builder \
            .appName(application_name) \
            .getOrCreate()
    
    # [START] Accessing S3 files
    #
    #
    # NOTE: AWS does not allow remote access to s3 files without aws credentials even if they are publicly accessible.
    
    # spark_context = spark_session.sparkContext
    # spark_context.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    
    # hadoop_conf = spark_context._jsc.hadoopConfiguration()
    # hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    # hadoop_conf.set("fs.s3a.access.key", "")
    # hadoop_conf.set("fs.s3a.secret.key", "")
    # hadoop_conf.set("fs.s3a.connection.maximum", "100000")
    #
    # src_rdd = sc.textFile("s3a://s3-eu-west-1.amazonaws.com/carnext-data-engineering-assignment/test_data/vehicle.csv0001_part_00.gz")
    #
    #
    # [END] Accessing S3 files
    return spark_session


def load_temp_table(spark_session, source_metadata):
    target_path_prefix = source_metadata["target_path_prefix"]
    source_file_list = source_metadata["source_file_list"]
    download_data(target_path_prefix, source_file_list)
    
    schema_fields = source_metadata["source_schema"]
    vehicle_dataframe = get_vehicle_files_df(spark_session, target_path_prefix + "vehicle.csv000*", schema_fields)
    vehicle_dataframe.createTempView("Source_Vehicle")


def get_vehicle_files_df(spark_session, source_file_path, schema_fields):
    print_log("[START] Loading following files to the temp table:" + str(source_file_path))
    source_df = spark_session \
        .read \
        .csv(
        source_file_path,
        header=True,
        schema=get_vehicle_file_schema(schema_fields)
    )
    print_log("[END] End of source file load")
    return source_df


def get_vehicle_file_schema(schema_fields):
    struct_fields = []
    for schema_field in schema_fields:
        struct_fields.append(
            StructField(
                schema_field['field_name'],
                get_field_type(schema_field['field_name'])
            )
        )
    return StructType(struct_fields)

def get_field_type(field_type_string):
    if field_type_string  == "String":
        return StringType()
    elif field_type_string =="Boolean":
        return BooleanType()
    else:
        return StringType()


def download_data(target_path, source_file_list):
    if not os.path.exists("./data/"):
        os.makedirs("./data/")
    
    for source_url in source_file_list:
        target_path = target_path + source_url[source_url.rfind("/"):]
        print_log("[START] Download file:" + target_path)
        download_file(source_url, target_path)
        print_log("[END] Download file:" + target_path)


def download_file(source_url, target_path):
    source_file_ref = requests.get(source_url)
    open(target_path, 'wb').write(source_file_ref.content)


if __name__ == "__main__":
    main()
