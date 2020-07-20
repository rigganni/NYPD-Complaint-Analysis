from datetime import datetime, timedelta
import urllib.request
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
import logging
import sys
from create_redshift_cluster_database import *
from create_bastion_host import *
from load_data_to_redshift import *
from cleanup_cluster import *

etl_filename = "transform_data.py"
s3_etl_uri = "s3://nypd-complaint/code/" + etl_filename
sql_filename = "emr_queries.py"
s3_sql_uri = "s3://nypd-complaint/code/" + sql_filename
s3_install_uri = "s3://nypd-complaint/code/install-requirements.sh"

# AWS hook used for S3 bucket creation & file downloads
aws_hook = AwsHook('aws_credentials')
credentials = aws_hook.get_credentials()
aws_access_key = credentials.access_key
aws_secret_key = credentials.secret_key

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 12),
    'email': ['riggan@pm.me'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG('nypd_complaint_analysis',
          default_args=DEFAULT_ARGS,
          schedule_interval="@once")

# Set steps to run once bootstrapping of EMR cluster is complete
# Note: use "sudo -H -u hadoop" to run python script to enable environment
# as if logging in as user "hadoop"
SPARK_STEPS = [{
    'Name': 'transform_weather',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar':
        'command-runner.jar',
        'Args': [
            'sudo', '-H', '-u', 'hadoop', 'bash', '-c',
            "cd /home/hadoop; aws s3 cp " + s3_sql_uri + " /home/hadoop/" +
            sql_filename + "; aws s3 cp " + s3_etl_uri + " /home/hadoop/" +
            etl_filename + " && sleep 30 && /usr/bin/python3 /home/hadoop/" +
            etl_filename + " aws " + aws_access_key + " " + aws_secret_key
        ]
    }
}]

# Set up EMR cluster config
# Adjust machine size / count here
# Must have Airflow variables ec2_key_name and ec2_subnet_id configured
JOB_FLOW_OVERRIDES = {
    'Name':
    'NYPDComplaints',
    'ReleaseLabel':
    'emr-6.0.0',
    'EbsRootVolumeSize':
    10,
    'Instances': {
        'MasterInstanceType': 'm5a.xlarge',
        'SlaveInstanceType': 'm5a.xlarge',
        'InstanceCount': 3,
        'TerminationProtected': False,
        'Ec2KeyName': Variable.get("ec2_key_name"),
        'Ec2SubnetId': Variable.get("ec2_subnet_id"),
        'KeepJobFlowAliveWhenNoSteps': False,
    },
    'BootstrapActions': [
        {
            'Name': 'Install Required pip Modules',
            'ScriptBootstrapAction': {
                'Path': s3_install_uri,
            }
        },
    ],
    'Steps':
    SPARK_STEPS,
    'JobFlowRole':
    'EMR_EC2_DefaultRole',
    'ServiceRole':
    'EMR_DefaultRole',
}


def download_store_s3(file_path, file_url, s3_key, **kwargs):
    """
    Download and store files required by NYPD complaint analysis

    Parameters:
    file_path (string): Local file path to save to
    file_url (string): URL of file to download
    s3_key (string): S3 key to use to store file
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
        s3.create_bucket("nypd-complaint", region_name="us-west-2")

    # Download data and store to S3-compatible backend
    file_exists = s3.check_for_key(s3_key, bucket_name="nypd-complaint")
    if not file_exists:
        logging.info("Started download and storage of " +
                     "s3://nypd-complaint/" + s3_key +
                     " to S3 compatible storage backend")

        # Download from url
        urllib.request.urlretrieve(file_url, file_path)
        s3.load_file(file_path, key=s3_key, bucket_name="nypd-complaint")
    else:
        logging.info("File " + "s3://nypd-complaint/" + s3_key +
                     " already exists")


# DAG task to download NYPD complaint data to S3 compatible storage backend
download_nypd_data = PythonOperator(
    task_id='download_nypd_complaint_store_s3',
    provide_context=True,
    python_callable=download_store_s3,
    op_kwargs={
        'file_path': '/tmp/nypd-complaint.csv',
        'file_url':
        'https://data.cityofnewyork.us/api/views/qgea-i56i/rows.csv?accessType=DOWNLOAD',
        's3_key': 'data/raw/nypd-complaint.csv'
    },
    dag=dag)

# DAG task to download NYC NOAA weather data to S3 compatible storage backend
download_nyc_noaa_data = PythonOperator(
    task_id='download_nyc_weather_store_s3',
    provide_context=True,
    python_callable=download_store_s3,
    op_kwargs={
        'file_path': '/tmp/nyc-weather.csv',
        'file_url':
        'https://github.com/rigganni/NYPD-Complaint-Analysis/raw/master/data/nyc-weather.csv',
        's3_key': 'data/raw/nyc-weather.csv'
    },
    dag=dag)

# Adapted from https://airflow.readthedocs.io/en/latest/_modules/airflow/providers/amazon/aws/example_dags/example_emr_job_flow_manual_steps.html
job_flow_creator = EmrCreateJobFlowOperator(
    task_id='create_job_flow',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_credentials',
    emr_conn_id='emr_default',
    dag=dag)

# Adapted from https://airflow.readthedocs.io/en/latest/_modules/airflow/providers/amazon/aws/example_dags/example_emr_job_flow_manual_steps.html
job_sensor = EmrJobFlowSensor(
    task_id='check_job_flow',
    job_flow_id=
    "{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    aws_conn_id='aws_credentials',
    dag=dag)

# DAG task to create RedShift cluster
create_redshift_cluster = PythonOperator(
    task_id='create_redshift_cluster',
    python_callable=create_redshift_cluster,
    dag=dag)

# DAG task to create RedShift bastion host for ssh tunneling
create_bastion_host = PythonOperator(task_id='create_ssh_bastion_host',
                                     python_callable=create_bastion_host,
                                     dag=dag)

# DAG task to drop RedShift tables if existing
drop_redshift_tables = PythonOperator(task_id='drop_redshift_tables',
                                      python_callable=run_queries,
                                      op_kwargs={
                                          'run_local': False,
                                          'type': 'drop'
                                      },
                                      provide_context=True,
                                      dag=dag)

# DAG task to create RedShift tables
create_redshift_tables = PythonOperator(task_id='create_redshift_tables',
                                        python_callable=run_queries,
                                        op_kwargs={
                                            'run_local': False,
                                            'type': 'create'
                                        },
                                        provide_context=True,
                                        dag=dag)

# DAG task to create RedShift tables
load_redshift_tables = PythonOperator(task_id='load_redshift_tables',
                                      python_callable=run_queries,
                                      op_kwargs={
                                          'run_local': False,
                                          'type': 'copy'
                                      },
                                      provide_context=True,
                                      dag=dag)

# DAG task to delete RedShift cluster if requested
cleanup_redshift_cluster = PythonOperator(task_id='cleanup_redshift_cluster',
                                          python_callable=cleanup_cluster,
                                          op_kwargs={'run_local': False},
                                          provide_context=True,
                                          dag=dag)

download_nypd_data >> job_flow_creator

# Create AWS RedShift cluster while transformations run on AWS EMR
download_nypd_data >> create_redshift_cluster >> drop_redshift_tables
download_nypd_data >> create_bastion_host >> drop_redshift_tables
download_nyc_noaa_data >> create_redshift_cluster >> drop_redshift_tables
download_nyc_noaa_data >> create_bastion_host >> drop_redshift_tables
job_sensor >> drop_redshift_tables
job_flow_creator >> job_sensor
drop_redshift_tables >> create_redshift_tables
create_redshift_tables >> load_redshift_tables
load_redshift_tables >> cleanup_redshift_cluster
