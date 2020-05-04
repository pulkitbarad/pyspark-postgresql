from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import requests
import os
from datetime import datetime

def get_spark_context(application_name):
    spark_conf = SparkConf().setAppName(application_name)
    spark_context = SparkContext(conf=spark_conf)

    # [START] Accessing S3 files
    #
    #
    # NOTE: AWS does not allow remote access to s3 files without aws credentials even if they are publicly accessible.
    # Un

    # conf = (
    #                 SparkConf()
    #                 .setAppName("FirstApp")
    #                 .set("spark.executor.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true")
    #                 .set("spark.driver.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true")
    #         )
    # sc = SparkContext(conf=conf)

    # sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    # hadoop_conf=sc._jsc.hadoopConfiguration()
    # hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    # hadoop_conf.set("fs.s3a.access.key", "")
    # hadoop_conf.set("fs.s3a.secret.key", "")
    # hadoop_conf.set("fs.s3a.connection.maximum", "100000")
    #
    # src_df = sc.textFile("s3a://s3-eu-west-1.amazonaws.com/carnext-data-engineering-assignment/test_data/vehicle.csv0001_part_00.gz")
    #
    #
    # [END] Accessing S3 files
    return spark_context


def get_source_file_list():
    return [
        # "https://s3-eu-west-1.amazonaws.com/carnext-data-engineering-assignment/test_data/vehicle.csv0001_part_00.gz",
        # "https://s3-eu-west-1.amazonaws.com/carnext-data-engineering-assignment/test_data/vehicle.csv0002_part_00.gz",
        # "https://s3-eu-west-1.amazonaws.com/carnext-data-engineering-assignment/test_data/vehicle.csv0003_part_00.gz",
        # "https://s3-eu-west-1.amazonaws.com/carnext-data-engineering-assignment/test_data/vehicle.csv0004_part_00.gz",
        # "https://s3-eu-west-1.amazonaws.com/carnext-data-engineering-assignment/test_data/vehicle.csv0005_part_00.gz",
        # "https://s3-eu-west-1.amazonaws.com/carnext-data-engineering-assignment/test_data/vehicle.csv0006_part_00.gz",
        "https://s3-eu-west-1.amazonaws.com/carnext-data-engineering-assignment/test_data/vehicle.csv0007_part_00.gz"
    ]


def download_data():
    if not os.path.exists("./data/"):
        os.makedirs("./data/")
      
    for source_url in get_source_file_list():
        target_path = "./data/" + source_url[source_url.rfind("/"):]
        print_log("[START] Download file:"+target_path)
        download_file(source_url, target_path)
        print_log("[END] Download file:"+target_path)


def download_file(source_url, target_path):
    source_file_ref = requests.get(source_url)
    open(target_path, 'wb').write(source_file_ref.content)

def print_log(statement):
    print( f'{datetime.now().strftime("%y/%m/%d %H:%M:%S")} ======CUSTOM====== {statement}')


def main():
    spark_context = get_spark_context("Test_application")
    download_data()
    df = spark_context.textFile("./data/vehicle.csv0007_part_00.gz")
    print_log("This is the number of records in the first file:"+str(df.count()))


if __name__ == "__main__":
    main()
