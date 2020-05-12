# Introduction

This document explains an application to ingest example vehicle data in csv format, transform and load it into Apache Spark and export the aggregated results into a Postgresql database.

# Prerequisites
* Docker-machine or Docker desktop installed and configured to use host OS disk.
* Python version 3 installed.
* External network access (through firewall) to the docker containers.

# Setup
Replicate the git repository or download the source code locally.
* Step 1: Pull the pyspark docker image
```bash
docker pull godatadriven/pyspark
docker pull bitnami/postgresql
```
* Step 2: Start the postgresql database container.
```bash
docker-compose up -d
```
* Step 3: Run the Entry_Point.py python file.
```bash
docker run --network "${PWD##*/}_app-tier" -v "$(pwd):/artifacts" godatadriven/pyspark --driver-class-path /artifacts/utils/postgresql-42.2.12.jar --jars /artifacts/utils/postgresql-42.2.12.jar /artifacts/Entry_Point.py
```

# Setup Verification
* Step 1: Connect to database container with credentials configured in docker-compose.yml file.
```bash
docker exec -it postgresql-server psql -d my_database -U my_user
```
* Step 2: Connect to database container with credentials configured in docker-compose.yml file.
```postgres-psql
SELECT * FROM PUBLIC.AGGREGATED_VEHICLE LIMIT 20;
```
If the query returns non-empty records then it means the setup was correct and the pipeline ran without syntactic errors.

# Design
This application has been created for the demonstration purpose only. To get started faster, we will use widely used docker container images for PySpark and PostgreSQL. As a result, we are restricted to the functionality available out of the box, which will be enough for our case.

## Assumptions and Choices
* Cloud Platform - AWS.

Since the source data is provided in AWS S3, we are assuming that is where most of the data is stored. If it turns out that we need to use another cloud platform, the choice of services and components need to reevaluated. For example, Amazon Redishift and Google Bigquery are both managed data warehousing services but they have different architecture and have different pricing models.
But the functional principles of the design should always be technology agnostic. For example, if the consumers of the platform deals with high volume then the application needs to scale horizontally.

* Source System: Nature of load (Incremental or Full) and History.

We are assuming that data will be delivered incrementally in batch mode. In case the complete history is provided each time then additional step needs to be added to pick up latest updated records with vehicle_id based on the latest load date (AUDIT_CREATED_AT).

* Data Quality and Data Governance.

There are some records with year as price class or vehicle id being null etc. But we are assuming that consumers will set the expectations for the data quality according to the their business case and we should not remove bad records.
However, we do add audit columns e.g. when the data was loaded, standard data quality checks like engine_capacity or age columns cannot be negative. We will flag the records that do not pass such tests.

* Data Model.

We are going to keep both types of the consumers in mind, the users(perhaps data scientists) who would prefer scale over low latency and the ones (perhaps operational systems) who would prefer low latency and strict consistency over volume of data.
We will review this topic in detail in a later section.

Now, that we are aware of assumptions that we are making and conscious choices we have made, let's take a look at the following diagram to understand the design better.

![](./HighLevelDesign.png)

Since our source data is in S3, to keep the data movement at minimum we should store transformed data in S3 as well. Currently, we are using apache spark in a docker container running on locally. however, the pyspark code we have can be run on AWS EMR cluster with little bit of modification. Local file system can be replaced by S3(as HDFS) and standalone spark installation with AWS EMR.

Spark configuration requires AWS credentials in order to natively access S3 even if the files are publicly available. Therefore, we will first download those file within docker container and then load into Spark.

After, we have loaded the source data into Spark, we load data into teh Staging table. In this table, we will add additional metadata columns AUDIT_CREATED_AT and AUDIT_CREATED_BY to help identify when each record was loaded as we append new data.

```python
    save_query_results(
...
        sql="""
            SELECT
                *,
                CURRENT_TIMESTAMP() AS AUDIT_CREATED_AT,
                "1_STAGING_VEHICLE" AS AUDIT_CREATED_BY
            FROM
                Source_Vehicle
        """,
...
    )
```

As you may have noticed, we are keeping metadata specific to the source file into vehicle.json file and are writing a generic code that can be used to load multiple files by just creating a new json metadata file with a new schema and the relevant information.
We don't want to apply any transformation or data manipulation at this stage because this is the first point in the pipeline where we have complete control of the incoming data.

Now, we will apply standardization to the staging table and load to the Standardization table. Currently, we are applying following types standardization to data:

* Column name changes: For example, changing the columns names, i.e. bodytype to body_type etc.
* Column data type: All the columns in the source data are of string we will change them to appropriate types.
* Standard data quality check: We will add some standard data quality checks i.e. regex to check whether date and number columns have proper value etc.

```python

def get_column_dq_check_expr(field):
...
    if (target_type == "date"):
        return "(TRIM(" + field_name + ") RLIKE " + "'[12]\\\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\\\\d|3[01])' OR " + field_name + " IS NULL)"
    elif (target_type == "integer"):
        return "(TRIM(" + field_name + ") RLIKE " + "'^\\\\d+$' OR " + field_name + " IS NULL)"
...
```

```python
    save_query_results(
...
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
    )
...    
```

All the steps we used so far are completely reusable, meaning once we create a new json metadata file with all the required information, we can load and standardize new tables without changing or redeploying the code.

We have only one table at this point, and data stored in Amazon Redshift or Apache Parquet files or Google Bigquery are columnar. So it makes sense to use the data in it's denormalized form. If we normalize the table, it is very likely that we will end up joining them in the end to perform the analysis. For most of the distributed and parallel computing frameworks join of large tables is an expensive operation.
Hence, we will keep the table in the denormalized form as it is in Standardized layer until we discover more entities in the model.

However, it is also possible that some downstream applications would like to access smaller/aggregated form of the data at a much lower latency than what Spark can offer. For, all such downsteam systems, we will create export of subsets of data into postgresql. The aggregated table we have is such an example.

Postgresql database container in our design can be replaced with services like  Amazon Redshift. Since the rest of our data is stored in S3, we can use Amazon Redshift spectrum to access both data stored in S3 and in Redshift. This use case is also possible in Google Bigquery with help of external tables on top of data stored in Google Cloud Storage.

```python

def aggregate_vehicle_data(spark_session,vehicle_metadata):
    non_aggregated_columns = [
        "make",
        "build_year",
        "price_class"
    ]
    
    export_query_results(
...
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
...
     )
...

def export_query_results(spark_session,sql,table_name,append_mode):

    result_dataframe = spark_session.sql(sql)

    #TODO: Move credentials to an encrypted file and create a separate database schema
    spark_session.sql(sql).write \
        .mode(append_mode)\
        .jdbc("jdbc:postgresql://postgresql-server:5432/my_database", "public."+table_name,
              properties={"driver":"org.postgresql.Driver","user": "my_user", "password": "Change123"})
...

```

Amazon Redshift is accessible using postgresql jdbc driver and therefore our code should be able to export data to that service with minimum changes.

For the demonstration purpose, we can pass sql query as an additional argument at the runtime after we have run the pipeline at least once, that will run the query on the stored data so that we can verify the results.

```python

def demo_query_execution(spark_session,sql,vehicle_metadata):
...
    #TODO: This is just a demo function; store the hive metastoe into persistent storage instead of reading the tables on every run.
    for table_name in table_list:
        saved_dataframe = spark_session.read.parquet(vehicle_metadata["data_dir"] + table_name)
        saved_dataframe.createTempView(table_name)
    
    spark_session.sql(sql).show(10)

```

We tried to justify the choices we made and it is never a bad idea to revisit them upon the discovery of relevant information about the users of the platform, upstream/downstream systems and the nature of data.

# Next Steps

* Either create a single docker image to combine pyspark and postgresql or customize the code to run on a managed spark service like AWS EMR or Google Cloud Dataproc.
* Refactor database write code and enable Entry_Point script to run queries on the database queries from command line argument.
* Save and load hive metastore from a persistent storage.
* Install Zeplin or Jypter with notebooks with standard code sections within the docker image.
* Ensure SSL through out the pipeline i.e. Spark, Database communications.
* Add the resource manager code, i.e. YARN or Mesos for automatic recovery of data from pipeline failures and auto restart strategy.
* Add unit tests and integration tests and automate the data verification after the pipeline execution.

