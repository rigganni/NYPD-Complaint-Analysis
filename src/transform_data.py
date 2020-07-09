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

config = configparser.ConfigParser()
config.read('/tmp/dl.cfg')

aws_s3_uri = config.get("AWS", "AWS_S3_URI")
aws_access_key = config.get("AWS", "AWS_ACCESS_KEY_ID")
aws_secret_key = config.get("AWS", "AWS_SECRET_ACCESS_KEY")

def create_spark_session():
    """
    Create SparkSession object

    Parameters:
    None

    Returns:
    spark (pyspark.sql.SparkSession): Spark session
    """

    # Adapted from https://www.jitsejan.com/setting-up-spark-with-minio-as-object-storage.html
    conf = (
        SparkConf()
        .setAppName("NYPD complaint Analysis")
        .set("spark.hadoop.fs.s3a.endpoint", aws_s3_uri)
        .set("spark.hadoop.fs.s3a.access.key", aws_access_key)
        .set("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
        .set("spark.hadoop.fs.s3a.path.style.access", True)
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )
    sc = SparkContext(conf=conf).getOrCreate()
    sqlContext = SQLContext(sc)
    spark = sqlContext.sparkSession
    return spark

def get_nypd_complaint_record_cnt(spark):
    """
    NYPD complaint analysis

    Parameters:
    spark (pyspark.sql.SparkSession): Spark session

    Returns:
    None
    """

    df = spark.read.csv("s3a://nypd-complaint/nypd-complaint.csv", inferSchema = True, header = True)
    df.printSchema()

    df.createOrReplaceTempView("nypd_complaint_analysis")

    record_cnt = spark.sql("""
    SELECT COUNT(1) as cnt
    FROM nypd_complaint_analysis;
    """)

    print(record_cnt.head())

    cmplnt_cnt_by_fr_dt = spark.sql("""
    SELECT CMPLNT_FR_DT, 
           COUNT(1) AS CNT
    FROM nypd_complaint_analysis
    GROUP BY CMPLNT_FR_DT
    ORDER BY CNT DESC;
    """)

    i = 1
    for index, row in cmplnt_cnt_by_fr_dt.select("*").toPandas().iterrows():
        if i > 10:
            break
        print(row["CMPLNT_FR_DT"] + " " + str(row["CNT"]))
        i += 1

def main():
    """
    Analyze NYPD complaints

    Parameters:
    None

    Returns:
    None
    """

    spark = create_spark_session()
    get_nypd_complaint_record_cnt(spark)

if __name__ == "__main__":
    main()
