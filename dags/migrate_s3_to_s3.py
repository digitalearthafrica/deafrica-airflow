from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import DAG, configuration
from textwrap import dedent
from pathlib import Path

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.sftp_operator import SFTPOperator, SFTPOperation
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.aws_sqs_hook import SQSHook
from airflow.contrib.sensors.aws_sqs_sensor import SQSSensor
import json

def get_parameter_value_from_variable(_dag_config, parameter_name, default_value=''):
    """
    Return parameter value from variable
    :param _dag_config: Name of the variable
    :param parameter_name: Parameter name
    :param default_value: Default value if parameter is empty
    :return: Parameter value for provided parameter name
    """
    if parameter_name in _dag_config and _dag_config[parameter_name]:
        parameter_value = _dag_config[parameter_name]
    elif default_value != '':
        parameter_value = default_value
    else:
        raise Exception("Missing necessary parameter '{}' in "
                        "variable '{}'".format(parameter_name, VARIABLE_NAME))
    return parameter_value

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 12, 1),
    'email': ['toktam.ebadi@ga.gov.au'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    "aws_conn_id": "deafrica_data_dev_migration",
    "s3bucket": "deafrica-data-dev",
    "s3_src": "s3://deafrica-data-dev/",    
    "sqs_queue": "https://sqs.us-west-2.amazonaws.com/565417506782/test_africa"
}

with DAG('migrate_s3_to_s3', default_args=default_args, schedule_interval='@once', catchup=False) as dag:

    check_sqs = SQSSensor(
        task_id='sqs_sensor',
        sqs_queue=dag.default_args['sqs_queue'],
        aws_conn_id = dag.default_args['aws_conn_id'],
        max_messages = 10,
        wait_time_seconds = 1
        )

    def copy_s3_objects(ti, **kwargs):
        s3_hook = S3Hook(aws_conn_id=dag.default_args['aws_conn_id'])        
        messages = ti.xcom_pull(key='messages', task_ids='sqs_sensor')
        print(messages)
        for m in messages['Messages']:
            body = json.loads(m['Body'])
            for rec in body['Records']:
                src_key = rec['s3']['object']['key']
                src_bucket = rec['s3']['bucket']['name']                
                s3_hook.copy_object(source_bucket_key=src_key, dest_bucket_key=src_key, 
                                     source_bucket_name=src_bucket, dest_bucket_name='africa-migration-test')

    read_messages = PythonOperator(
        task_id='read_messages_script',
        provide_context=True,
        python_callable=copy_s3_objects
        )

    # Use arrows to set dependencies between tasks
    check_sqs >> read_messages