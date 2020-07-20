import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, types
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import subprocess
import sys
import pandas as pd
import numpy as np
import boto3
from emr_queries import transform_table_queries


def create_spark_session(env="local"):
    """
    Create SparkSession object

    Parameters:
    env (string): Environment (i.e. local or aws). Default: local

    Returns:
    spark (pyspark.sql.SparkSession): Spark session
    """

    config = configparser.ConfigParser()
    if env == "local":
        config.read('/tmp/local.cfg')
        aws_s3_uri = config.get("AWS", "AWS_S3_URI")
        aws_access_key = config.get("AWS", "AWS_ACCESS_KEY_ID")
        aws_secret_key = config.get("AWS", "AWS_SECRET_ACCESS_KEY")

    # Set Spark connections
    if env == "local":
        # Adapted from https://www.jitsejan.com/setting-up-spark-with-minio-as-object-storage.html
        conf = (SparkConf().setAppName("NYPD complaint Analysis").set(
            "spark.hadoop.fs.s3a.endpoint", aws_s3_uri).set(
                "spark.hadoop.fs.s3a.access.key", aws_access_key).set(
                    "spark.hadoop.fs.s3a.secret.key", aws_secret_key).set(
                        "spark.hadoop.fs.s3a.path.style.access",
                        True).set("spark.hadoop.fs.s3a.impl",
                                  "org.apache.hadoop.fs.s3a.S3AFileSystem"))
        sc = SparkContext(conf=conf).getOrCreate()
        sqlContext = SQLContext(sc)
        spark = sqlContext.sparkSession
    else:
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .appName("NYPD Complaint Analysis") \
            .getOrCreate()

    return spark


def test_redshift_dataset(spark,
                          env="local",
                          dataset="",
                          s3_uri="",
                          sql="",
                          expected_value=0):
    """
    Tests of EMR-created CSV files to be imported into AWS RedShift

    Parameters:
    spark (pyspark.sql.SparkSession): Spark session
    env (string): Environment (i.e. local or aws). Default: local
    dataset (string): Dataset to transform & write to CSV
    s3_uri (string): S3 URI of source data to transform
    sql (string): SQL statement to transform dataset
    expected_value (int): Expected value of test

    Returns:
    None
    """

    # Set local environment variables if running locally
    if env == "local":
        config = configparser.ConfigParser()
        config.read('/tmp/local.cfg')
        aws_s3_uri = config.get("AWS", "AWS_S3_URI")
        aws_access_key = config.get("AWS", "AWS_ACCESS_KEY_ID")
        aws_secret_key = config.get("AWS", "AWS_SECRET_ACCESS_KEY")

    # Create Spark temp view from newly created CSV file
    df = spark.read.csv(s3_uri, inferSchema=True, header=True)
    df.createOrReplaceTempView(dataset)

    result = spark.sql(sql).toPandas()['result'].iloc[0]

    # Perform unit tes
    assert result == expected_value


def create_csv_for_redshift(spark,
                            env="local",
                            aws_access_key="",
                            aws_secret_key="",
                            dataset="",
                            s3_uri="",
                            sql=""):
    """
    NYPD complaint analysis

    Parameters:
    spark (pyspark.sql.SparkSession): Spark session
    env (string): Environment (i.e. local or aws). Default: local
    aws_access_key (string): AWS access key
    aws_secret_key (string): AWS secret key
    dataset (string): Dataset to transform & write to CSV
    s3_uri (string): S3 URI of source data to transform
    sql (string): SQL statement to transform dataset

    Returns:
    None
    """

    # Set local environment variables if running locally
    if env == "local":
        config = configparser.ConfigParser()
        config.read('/tmp/local.cfg')
        aws_s3_uri = config.get("AWS", "AWS_S3_URI")
        aws_access_key = config.get("AWS", "AWS_ACCESS_KEY_ID")
        aws_secret_key = config.get("AWS", "AWS_SECRET_ACCESS_KEY")

    df = spark.read.csv(s3_uri, inferSchema=True, header=True)

    df.createOrReplaceTempView(dataset)

    result = spark.sql(sql)

    csv_file = "/tmp/" + dataset + ".csv"
    s3_key = "data/transform/" + dataset + ".csv"

    # Write Pandas dataframe to create single CSV file
    # Utilizing Spark's csv write function creates many files
    result.toPandas().to_csv(csv_file, header=True, index=False)

    # Set up boto3 S3 resource based on execution environment
    if env == "local":
        s3 = boto3.resource('s3',
                            endpoint_url=aws_s3_uri,
                            aws_access_key_id=aws_access_key,
                            aws_secret_access_key=aws_secret_key)
    else:
        s3 = boto3.resource('s3',
                            aws_access_key_id=aws_access_key,
                            aws_secret_access_key=aws_secret_key)

    # Write CSV file to S3
    s3.Bucket("nypd-complaint").upload_file(csv_file, s3_key)


def main():
    """
    Analyze NYPD complaints

    Parameters:
    env (string): Environment (i.e. local or aws). Default: local
    aws_access_key (string): AWS access key
    aws_secret_key (string): AWS secret key

    Returns:
    None
    """

    # Obtain command line arguments
    env = sys.argv[1]
    aws_access_key = sys.argv[2]
    aws_secret_key = sys.argv[3]
    spark = create_spark_session(env)

    # Iterate through each dataset and transform into dimensional model
    for dataset, details in transform_table_queries.items():
        create_csv_for_redshift(spark,
                                env,
                                aws_access_key,
                                aws_secret_key,
                                dataset=dataset,
                                s3_uri="s3a://nypd-complaint/data/raw/" +
                                details["source_data"],
                                sql=details["query"])

    # Iterate through each newly created dataset and run unit tests
    for dataset, details in test_table_queries.items():
        test_redshift_dataset(spark,
                              env,
                              dataset=dataset,
                              s3_uri="s3a://nypd-complaint/data/transform/" +
                              details["source_data"],
                              sql=details["query"],
                              expected_value=details["expected_value"])


if __name__ == "__main__":
    main()
