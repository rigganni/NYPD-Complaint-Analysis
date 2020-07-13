from datetime import datetime, timedelta
import urllib.request
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor
import logging
import sys

etl_filename = "transform_data.py"
s3_etl_uri = "s3://nypd-complaint/" + etl_filename
s3_install_uri = "s3://nypd-complaint/install-requirements.sh"

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
dag = DAG('nypd_complaint_analysis', default_args=DEFAULT_ARGS,
          schedule_interval="@once")

# Set steps to run once bootstrapping of EMR cluster is complete
# Note: use "sudo -H -u hadoop" to run python script to enable environment
# as if logging in as user "hadoop"
SPARK_STEPS = [
    {
        'Name': 'transform_weather',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'sudo',
                '-H',
                '-u',
                'hadoop',
                'bash',
                '-c',
                "aws s3 cp " + s3_etl_uri + " /home/hadoop/" + etl_filename + " && sleep 30 && /usr/bin/python3 /home/hadoop/" + etl_filename + " aws " + aws_access_key + " " + aws_secret_key
            ]
        }
    }
 ]

# Set up EMR cluster config
# Adjust machine size / count here
# Must have Airflow variables ec2_key_name and ec2_subnet_id configured
JOB_FLOW_OVERRIDES = {
    'Name': 'NYPDComplaints',
    'ReleaseLabel': 'emr-6.0.0',
    'EbsRootVolumeSize': 10,
    'Instances': {
        'MasterInstanceType': 'm5a.xlarge',
        'SlaveInstanceType': 'm5a.xlarge',
        'InstanceCount': 2,
        'TerminationProtected': False,
        'Ec2KeyName': Variable.get("ec2_key_name"),
        'Ec2SubnetId': Variable.get("ec2_subnet_id"),
        'KeepJobFlowAliveWhenNoSteps': True,
    },
    'BootstrapActions': [
        {
            'Name': 'Install Required pip Modules',
            'ScriptBootstrapAction': {
                'Path': s3_install_uri,
            }
        },
    ],
    'Steps': SPARK_STEPS,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
}

def download_store_s3(file_path, file_url, filename, **kwargs):
    """
    Download and store files required by NYPD complaint analysis

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

    # Download data and store to S3-compatible backend
    file_exists = s3.check_for_key(filename, bucket_name="nypd-complaint")
    if not file_exists:
        logging.info("Started download and storage of " + filename + " to S3 compatible storage backend")

        # Download from url
        urllib.request.urlretrieve(file_url, file_path)
        s3.load_file(file_path,
                     key=filename,
                     bucket_name="nypd-complaint")
    else:
        logging.info("File " + filename + " already exists")

def load_to_postgres():
    #df = pd.read_csv
    pass

# DAG task to download NYPD complaint data to S3 compatible storage backend
t1 = PythonOperator(
    task_id='download_nypd_complaint_store_s3',
    provide_context=True,
    python_callable=download_store_s3,
    op_kwargs={'file_path': '/tmp/nypd-complaint.csv',
               'file_url': 'https://data.cityofnewyork.us/api/views/qgea-i56i/rows.csv?accessType=DOWNLOAD',
               'filename': 'nypd-complaint.csv'
               },
    dag=dag
)

# DAG task to download NYC NOAA weather data to S3 compatible storage backend
#t2 = PythonOperator(
#    task_id='download_nyc_weather_store_s3',
#    provide_context=True,
#    python_callable=download_store_s3,
#    params={'file_path': '/tmp/nyc-weather.csv',
#            'file_url': 'https://github.com/rigganni/NYPD-Complaint-Analysis/raw/master/data/nyc-weather.csv',
#            'filename': 'nyc-weather.csv'
#            },
#    dag=dag
#)

# DAG task to download pip requirements file to S3
#t3 = PythonOperator(
#    task_id='download_install_requirements_store_s3',
#    provide_context=True,
#    python_callable=download_store_s3,
#    params={'file_path': '/tmp/install-requirements.sh',
#            'file_url': 'https://github.com/rigganni/NYPD-Complaint-Analysis/raw/master/src/install-requirements.sh',
#            'filename': 'install-requirements.sh'
#            },
#    dag=dag
#)

job_flow_creator = EmrCreateJobFlowOperator(
    task_id='create_job_flow',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_credentials',
    emr_conn_id='emr_default',
    dag=dag
)

job_sensor = EmrJobFlowSensor(
    task_id='check_job_flow',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

t1 >> job_flow_creator 
#t2 >> job_flow_creator
#t3 >> job_flow_creator
job_flow_creator >> job_sensor
