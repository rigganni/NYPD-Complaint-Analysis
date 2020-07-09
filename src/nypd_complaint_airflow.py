from datetime import datetime, timedelta
import urllib.request

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import logging
import sys

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 8),
    'email': ['riggan@pm.me'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('nypd_complaint_analysis', default_args=DEFAULT_ARGS,
          schedule_interval="@once")


def download_store_nypd_complaint(**kwargs):
    """
    Download and store NYPD complaint data

    Parameters:
    kwargs: Keyword arguments

    Returns:
    None
    """

    env = kwargs['dag_run'].conf['env']
    if env == 'local':
        # S3 Hook should called "local_minio" should be created already on Airflow
        s3 = S3Hook('local_minio')
        logging.info("*** Running local ***")
    elif env == 'aws':
        # S3 Hook should called "aws_s3" should be created already on Airflow
        s3 = S3Hook('aws_s3')
        logging.info("*** Running on AWS ***")

    # Check if bucket already exists
    # If not, create new bucket
    bucket_exists = s3.check_for_bucket("nypd-complaint")
    if not bucket_exists:
        s3.create_bucket("nypd-complaint")

    # Download NYPD complaint data and store to S3-compatible backend
    file_exists = s3.check_for_key(f"nypd-complaint.csv", bucket_name="nypd-complaint")
    if not file_exists:

        logging.info("Started download and storage of NYPD complaint data to S3 compatible storage backend")

        # Download NYPD complaint data
        urllib.request.urlretrieve("https://data.cityofnewyork.us/api/views/qgea-i56i/rows.csv?accessType=DOWNLOAD", "/tmp/nypd-complaint.csv")
        s3.load_file("/tmp/nypd-complaint.csv",
                     key=f"nypd-complaint.csv",
                     bucket_name="nypd-complaint")
    else:

        logging.info("File nypd-complaint.csv already exists")

    # Download NYC weather data and store to S3-compatible backend
    file_exists = s3.check_for_key(f"nyc-weather.csv", bucket_name="nypd-complaint")
    if not file_exists:

        logging.info("Started download and storage of NYC weather data to S3 compatible storage backend")

        # Download NYC weather data
        urllib.request.urlretrieve("https://github.com/rigganni/NYPD-Complaint-Analysis/raw/master/data/nyc-weather.csv", "/tmp/nyc-weather.csv")
        s3.load_file("/tmp/nyc-weather.csv",
                     key=f"nyc-weather.csv",
                     bucket_name="nypd-complaint")
    else:

        logging.info("File nyc-weather.csv already exists")

def load_to_postgres():
    #df = pd.read_csv
    pass

# DAG task to download to S3 compatible storage backend
t1 = PythonOperator(
    task_id='download_nypd_complaint_store_s3',
    provide_context=True,
    python_callable=download_store_nypd_complaint,
    dag=dag
)
