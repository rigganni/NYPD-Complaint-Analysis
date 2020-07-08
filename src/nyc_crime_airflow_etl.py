from datetime import datetime, timedelta
import urllib.request

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import logging

DEFAULT_ARGS = {
    'owner': 'Nick Riggan',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 8),
    'email': ['riggan@pm.me'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('nyc_crime_analysis', default_args=DEFAULT_ARGS,
          schedule_interval="@once")


def download_store_nyc_crime(**kwargs):
    """
    Download and store NYC crime data

    Parameters:
    kwargs: Keyword arguments

    Returns:
    None
    """

    # S3 Hook should called "local_minio" should be created already on Airflow
    s3 = S3Hook('local_minio')

    # Check if bucket already exists
    # If not, create new bucket
    bucket_exists = s3.check_for_bucket("nyc-crime")
    if not bucket_exists:
        s3.create_bucket("nyc-crime")

    file_exists = s3.check_for_key(f"nyc-crime-new-airflow.csv", bucket="nyc-crime")
    if not file_exists:

        logging.info("Started download and storage of nyc crime data to S3 compatible storage backend")

        # Download NYC crime data
        urllib.request.urlretrieve("https://data.cityofnewyork.us/api/views/qgea-i56i/rows.csv?accessType=DOWNLOAD", "/tmp/nyc-crime-new-airflow.csv")
        s3.load_file("/tmp/nyc-crime-new-airflow.csv",
                     key=f"nyc-crime-new-airflow.csv",
                     bucket_name="nyc-crime")
    else:

        logging.info("File already exists")


# DAG task to download to S3 compatible storage backend
t1 = PythonOperator(
    task_id='download_nyc_crime_store_s3',
    provide_context=True,
    python_callable=download_store_nyc_crime,
    dag=dag
)
