from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import requests
import os
import json
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def main():
    spark_session = get_spark_session("Test_application")
    
    file_metadata = json.load(open("/artifacts/metadata/vehicle.json"))
    vehicle_metadata = file_metadata["vehicle"]
    if(len(sys.argv)==2):
        demo_query_execution(
            spark_session=spark_session,
            sql=sys.argv[1],
            vehicle_metadata=vehicle_metadata
        )
    else:
        load_temp_table(spark_session=spark_session,source_metadata=vehicle_metadata)
        standardize_vehicle_data(spark_session=spark_session,vehicle_metadata=vehicle_metadata)
        aggregate_vehicle_data(spark_session=spark_session,vehicle_metadata=vehicle_metadata)


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
    target_path_prefix = source_metadata["temp_dir"]
    source_file_list = source_metadata["source_file_list"]
    download_data(target_path_prefix, source_file_list)
    
    save_query_results(
        spark_session=spark_session,
        data_dir=source_metadata["data_dir"],
        dataframe=get_vehicle_files_df(
                    spark_session=spark_session,
                    source_file_path=target_path_prefix + "vehicle.csv000*",
                    schema_fields=source_metadata["source_schema"]
                ),
        sql=None,
        table_name="Source_Vehicle",
        partition_column=None,
        append_mode="append"
    )

    save_query_results(
        spark_session=spark_session,
        data_dir=source_metadata["data_dir"],
        dataframe=None,
        sql="""
            SELECT
                *,
                CURRENT_TIMESTAMP() AS AUDIT_CREATED_AT,
                "1_STAGING_VEHICLE" AS AUDIT_CREATED_BY
            FROM
                Source_Vehicle
        """,
        table_name="Staged_Vehicle_Temp",
        partition_column=None,
        append_mode="append"
    )

    save_query_results(
        spark_session=spark_session,
        data_dir=source_metadata["data_dir"],
        dataframe=None,
        sql="""
            SELECT
                *
            FROM
                Staged_Vehicle_Temp AS Outer_Src
            WHERE
                AUDIT_CREATED_AT = (
                    SELECT
                        MAX(AUDIT_CREATED_AT)
                    FROM
                        Staged_Vehicle_Temp AS Inner_Src
                    WHERE
                        Outer_Src.vehicle_id = Inner_Src.vehicle_id
                )
        """,
        table_name="Staged_Vehicle",
        partition_column=None,
        append_mode="overwrite"
    )


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
        target_path = target_path_prefix + source_url[source_url.rfind("/")+1:]
        print_log("[START] Download file:" + target_path)
        download_file(source_url, target_path)
        print_log("[END] Download file:" + target_path)


def download_file(source_url, target_path):
    source_file_ref = requests.get(source_url)
    open(target_path, "wb").write(source_file_ref.content)

def save_query_results(spark_session, data_dir, dataframe, sql, table_name, partition_column, append_mode):

    result_dataframe = None

    if(dataframe !=None):
        result_dataframe = dataframe
    else:
        result_dataframe = spark_session.sql(sql)

    result_dataframe_writer = result_dataframe.write

    if(partition_column != None):
        print_log(f'Partitioning the table {table_name} by {partition_column} before saving.')
        result_dataframe_writer.partitionBy(partition_column)

    result_dataframe.write.mode(append_mode).saveAsTable(table_name,path=data_dir+table_name)


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

    save_query_results(
        spark_session=spark_session,
        data_dir=vehicle_metadata["data_dir"],
        dataframe=None,
        sql="""
            SELECT
                """ + ",\n\t".join(data_type_conversion_select_list) + """,
                """ + " AND ".join(standard_dq_check_expr_list) + """ AS DQ_STD_TYPE_CHECK,
                """ + get_non_negative_check_expr() + """ AS DQ_NON_NEGATIVE_CHECK,
                CURRENT_TIMESTAMP() AS AUDIT_CREATED_AT,
                '2_STANDARDIZATION_VEHICLE' AS AUDIT_CREATED_BY
            FROM
                Staged_Vehicle
        """,
        table_name="Standardized_Vehicle_Temp",
        partition_column=None,
        append_mode="append"
    )

    save_query_results(
        spark_session=spark_session,
        data_dir=vehicle_metadata["data_dir"],
        dataframe=None,
        sql="""
            SELECT
                *
            FROM
                Standardized_Vehicle_Temp AS Outer_Src
            WHERE
                AUDIT_CREATED_AT = (
                    SELECT
                        MAX(AUDIT_CREATED_AT)
                    FROM
                        Standardized_Vehicle_Temp AS Inner_Src
                    WHERE
                        Outer_Src.vehicle_id = Inner_Src.vehicle_id
                )
        """,
        table_name="Standardized_Vehicle",
        partition_column=None,
        append_mode="overwrite"
    )

    # print_log(standardized_vehicle_dataframe.show(5))


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
    
    if (target_type == "date"):
        return "(TRIM(" + field_name + ") RLIKE " + "'[12]\\\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\\\\d|3[01])' OR " + field_name + " IS NULL)"
    elif (target_type == "integer"):
        return "(TRIM(" + field_name + ") RLIKE " + "'^\\\\d+$' OR " + field_name + " IS NULL)"
    elif (target_type == "double"):
        return "(TRIM(" + field_name + ") RLIKE " + "'^\\\\d*(\\\\.\\\\d+)?$' OR " + field_name + " IS NULL)"
    elif (target_type == "boolean"):
        return "(LOWER(TRIM(" + field_name + ")) IN " + "('true','t','1','yes','y','false','f','0','no','n') OR " + field_name + " IS NULL)"
    else:
        return ""

def aggregate_vehicle_data(spark_session,vehicle_metadata):
    non_aggregated_columns = [
        "make",
        "build_year",
        "price_class"
    ]
    
    export_query_results(
        spark_session=spark_session,
        sql="""
            SELECT
                COUNT(*) AS num_of_vehicles,
                SUBSTRING(country, 3) as COUNTRY_CODE,
                """ + ",\n\t\t".join(non_aggregated_columns) + """,
                CURRENT_TIMESTAMP() AS AUDIT_CREATED_AT,
                "3_AGGREGATION_VEHICLE" AS AUDIT_CREATED_BY
            FROM
                Standardized_Vehicle
            GROUP BY
                """ + ",\n\t\t".join(str(x) for x in range(2, len(non_aggregated_columns) + 4)) + """
            ORDER BY
               1 DESC,
               4 DESC
        """,
        table_name="Aggregated_Vehicle",
        append_mode="overwrite"
     )


def export_query_results(spark_session,sql,table_name,append_mode):

    result_dataframe = spark_session.sql(sql)

    #TODO: Move credentials to an encrypted file and create a separate database schema
    spark_session.sql(sql).write \
        .mode(append_mode)\
        .jdbc("jdbc:postgresql://postgresql-server:5432/my_database", "public."+table_name,
              properties={"driver":"org.postgresql.Driver","user": "my_user", "password": "Change123"})


def demo_query_execution(spark_session,sql,vehicle_metadata):
    table_list = [
        "Source_Vehicle",
        "Staged_Vehicle",
        "Standardized_Vehicle"
    ]

    for table_name in table_list:
        saved_dataframe = spark_session.read.parquet(vehicle_metadata["data_dir"] + table_name)
        saved_dataframe.createTempView(table_name)
    
    spark_session.sql(sql).show(10)

if __name__ == "__main__":
    main()
