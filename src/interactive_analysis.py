# File used to create interactive analysis session on AWS EMR cluster

import configparser
import boto3
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

env = sys.argv[1]
nypd_cmplnt_file = "s3a://nypd-complaint/data/raw/nypd-complaint.csv"
nyc_weather_file = "s3a://nypd-complaint/data/raw/nyc-weather.csv"

if env == "local":
    config = configparser.ConfigParser()
    config.read('/tmp/dl.cfg')

    aws_s3_uri = config.get("AWS", "AWS_S3_URI")
    aws_access_key = config.get("AWS", "AWS_ACCESS_KEY_ID")
    aws_secret_key = config.get("AWS", "AWS_SECRET_ACCESS_KEY")

    conf = (SparkConf().setAppName("NYPD complaint Analysis").set(
        "spark.hadoop.fs.s3a.endpoint",
        aws_s3_uri).set("spark.hadoop.fs.s3a.access.key", aws_access_key).set(
            "spark.hadoop.fs.s3a.secret.key", aws_secret_key).set(
                "spark.hadoop.fs.s3a.path.style.access",
                True).set("spark.hadoop.fs.s3a.impl",
                          "org.apache.hadoop.fs.s3a.S3AFileSystem"))
    sc = SparkContext(conf=conf).getOrCreate()
    sqlContext = SQLContext(sc)
    spark = sqlContext.sparkSession
else:
    # Add environment variable based on https://stackoverflow.com/questions/46740670/no-filesystem-for-scheme-s3-with-pyspark
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2 pyspark-shell'
    spark = SparkSession \
        .builder \
        .appName("NYPD Complaint Analysis") \
        .getOrCreate()

nypd_cmplnt_df = spark.read.csv(nypd_cmplnt_file,
                                inferSchema=True,
                                header=True)
nypd_cmplnt_df.createOrReplaceTempView("nypd_cmplnt")

nyc_weather_df = spark.read.csv(nyc_weather_file,
                                inferSchema=True,
                                header=True)
nyc_weather_df.createOrReplaceTempView("nyc_weather")

pd.DataFrame(spark.sql("SELECT COUNT(1) AS cnt FROM nyc_weather").collect())

pd.DataFrame(spark.sql("SELECT COUNT(1) AS cnt FROM nyc_weather").collect(),
             columns=['cnt']).cnt[0]


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
    s3_key = dataset + ".csv"

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
