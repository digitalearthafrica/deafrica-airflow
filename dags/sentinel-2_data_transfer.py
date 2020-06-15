"""
# Migrate(copy) data between S3 buckets

DAG to periodically check a SQS and copy new data to Cape Town
The SQS is subscribed to the following SNS topic:
arn:aws:sns:us-west-2:482759440949:cirrus-dev-publish
In case where the queue is empty, a timeout policy is applied to kill this DAG
"""
import json
import re

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.aws_sqs_sensor import SQSSensor
from airflow.hooks.S3_hook import S3Hook

default_args = {
    'owner': 'Airflow',
    "start_date": datetime(2020, 6, 12),
    'email': ['toktam.ebadi@ga.gov.au'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    "aws_conn_id": "deafrica_data_dev_migration",
    "dest_bucket_name": "africa-migration-test",
    "src_bucket_name": "sentinel-cogs",
    "schedule_interval": '*/5 * * * *',
    "sqs_queue": ("https://sqs.us-west-2.amazonaws.com/565417506782/"
                  "deafrica-prod-eks-sentinel-2-data-transfer")
}

def extract_src_key(src_url):
    """
    Extract object key from specified url path e.g.
    https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/
    2020/S2A_38PKS_20200609_0_L2A/B11.tif
    :param src_url: Full http path of the object
    :return: Object path relative to the base bucket
    """
    matches = (re.finditer("/", src_url))
    matches_positions = [match.start() for match in matches]
    start = matches_positions[2] + 1
    return src_url[start:]

def copy_s3_objects(ti, **kwargs):
    """
    Copy objects from a s3 bucket to another s3 bucket.
    :param ti: Task instance
    """
    s3_hook = S3Hook(aws_conn_id=dag.default_args['aws_conn_id'])
    messages = ti.xcom_pull(key='messages', task_ids='sqs_sensor')

    for rec in messages['Messages']:
        body = json.loads(rec['Body'])
        message = json.loads(body['Message'])

        # Extract URL of the json file
        urls = [message["links"][0]["href"]]
        # Add URL of .tif files
        urls.extend([v["href"] for k, v in message["assets"].items() if "geotiff" in v['type']])
        for src_url in urls:
            src_key = extract_src_key(src_url)
            s3_hook.copy_object(source_bucket_key=src_key,
                                dest_bucket_key=src_key,
                                source_bucket_name=default_args['src_bucket_name'],
                                dest_bucket_name=default_args['dest_bucket_name'])
            print("Copied scene:", src_key)

with DAG('sentinel-2_data_transfer', default_args=default_args,
         schedule_interval=default_args['schedule_interval'],
         tags=["Sentinel-2", "transfer"], catchup=False,
         dagrun_timeout=timedelta(seconds=60*3)) as dag:

    PROCESS_SQS = SQSSensor(
        task_id='sqs_sensor',
        sqs_queue=dag.default_args['sqs_queue'],
        aws_conn_id=dag.default_args['aws_conn_id'],
        max_messages=5,
        wait_time_seconds=20,
        execution_timeout=timedelta(seconds=20)
    )

    COPY_OBJECTS = PythonOperator(
        task_id='copy_scenes',
        provide_context=True,
        python_callable=copy_s3_objects
    )

    process_sqs >> copy_objects
