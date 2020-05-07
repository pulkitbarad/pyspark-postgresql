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
    
    staged_vehicle_dataframe = spark_session.sql("""
        SELECT
            *,
            CURRENT_TIMESTAMP() AS AUDIT_CREATED_AT,
            "STAGING_STEP_VEHICLE" AS AUDIT_CREATED_BY
        FROM
            Source_Vehicle
    """)
    staged_vehicle_dataframe.createTempView("Staged_Vehicle")
    standardize_vehicle_data(spark_session,vehicle_metadata)


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
                schema_field["source_name"],
                get_field_type(schema_field["source_type"])
            )
        )
    return StructType(struct_fields)

def get_field_type(field_type_string):
    if field_type_string  == "String":
        return StringType()
    elif field_type_string =="Boolean":
        return BooleanType()
    
    # TODO: Current source file has all the columns in string tpye but more data type conversion are to be added.
    # Reference: https://spark.apache.org/docs/latest/sql-reference.html#data-types
    else:
        return StringType()


def download_data(target_path_prefix, source_file_list):
    if not os.path.exists(target_path_prefix):
        os.makedirs(target_path_prefix)
    
    for source_url in source_file_list:
        target_path = target_path_prefix + source_url[source_url.rfind("/"):]
        print_log("[START] Download file:" + target_path)
        download_file(source_url, target_path)
        print_log("[END] Download file:" + target_path)


def download_file(source_url, target_path):
    source_file_ref = requests.get(source_url)
    open(target_path, "wb").write(source_file_ref.content)


def standardize_vehicle_data(spark_session, vehicle_metadata):
    vehicle_schema_fields = vehicle_metadata["source_schema"]
    data_type_conversion_select_list = \
        list(
            map(
                lambda field: get_column_cast_expression(field) + " AS " + field["target_name"],
                vehicle_schema_fields
            )
        )
    standard_dq_check_expr_list = \
        list(
            map(
                lambda field: get_column_dq_check_expr(field),
                vehicle_schema_fields
            )
        )
    standard_dq_check_expr_list = \
        list(
            filter(
                lambda exp: len(exp) > 0,
                standard_dq_check_expr_list
            )
        )
    sql = """
            SELECT
                """ + ",\n\t".join(data_type_conversion_select_list) + """,
                """ + " AND ".join(standard_dq_check_expr_list) + """ AS DQ_STD_TYPE_CHECK,
                """ + get_non_negative_check_expr() + """ AS DQ_NON_NEGATIVE_CHECK,
                CURRENT_TIMESTAMP() AS AUDIT_CREATED_AT,
                'STANDARDIZATION_STEP_VEHICLE' AS AUDIT_CREATED_BY
            FROM
                Staged_Vehicle
        """
    print(sql)
    standardized_vehicle_dataframe = spark_session.sql(sql)
    
    print_log(standardized_vehicle_dataframe.take(1))


def get_non_negative_check_expr():
    field_names = [
        "engine_capacity",
        "engine_power",
        "cylindercapacity",
        "horsepower",
        "number_of_doors",
        "number_of_seats",
        "milage",
        "age"
    ]
    return " AND ".join(
        list(
            map(
                lambda field_name: f'({field_name} IS NULL OR {field_name} >= 0)',
                field_names
            )
        )
    )


def get_column_cast_expression(field):
    field_name = field["source_name"]
    target_type = field["target_type"].lower()
    if (target_type == "string"):
        return f'TRIM({field_name})'
    elif (target_type == "date"):
        return f'CAST({field_name} AS DATE)'
    elif (target_type == "integer"):
        return f'CAST({field_name} AS INTEGER)'
    elif (target_type == "double"):
        return f'CAST({field_name} AS DOUBLE)'
    elif (target_type == "boolean"):
        return f'CAST({field_name} AS BOOLEAN)'
    else:
        return field["source_name"]


def get_column_dq_check_expr(field):
    field_name = field["source_name"]
    target_type = field["target_type"].lower()
    
    # if (target_type == "date"):
    #     return "(TRIM(" + field_name + ") RLIKE " + "'[12]\\\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\\\\d|3[01])' OR " + field_name + " IS NULL)"
    if (target_type == "integer"):
        return "(TRIM(" + field_name + ") RLIKE " + "'^\\\\d+$' OR " + field_name + " IS NULL)"
    elif (target_type == "double"):
        return "(TRIM(" + field_name + ") RLIKE " + "'^\\\\d*(\\\\.\\\\d+)?$' OR " + field_name + " IS NULL)"
    elif (target_type == "boolean"):
        return "(LOWER(TRIM(" + field_name + ")) IN " + "('true','t','1','yes','y','false','f','0','no','n') OR " + field_name + " IS NULL)"
    else:
        return ""


if __name__ == "__main__":
    main()
