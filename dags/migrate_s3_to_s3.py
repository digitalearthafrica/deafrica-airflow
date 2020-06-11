"""
# Migrate(copy) data between S3 buckets

DAG to periodically check a SQS and copy new data to Cape Town
The SQS is subscribed to the following SNS topic: arn:aws:sns:us-west-2:482759440949:cirrus-dev-publish
In case where the queue is empty, a timeout policy is applied to kill the DAG
"""

from datetime import datetime, timedelta
from airflow import DAG, configuration
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.aws_sqs_sensor import SQSSensor
from airflow.hooks.S3_hook import S3Hook

import json
import logging
import re


def extract_src_key(src_url):
    matches  = (re.finditer("/", src_url))
    matches_positions = [match.start() for match in matches]
    start = matches_positions[2] + 1
    return src_url[start:]

def extract_src_bucket(src_url):
    return src_url[src_url.find("//") + 2: src_url.find(".s3")]

default_args = {
    'owner': 'Airflow',
    "start_date": datetime(2017, 2, 1),
    'email': ['toktam.ebadi@ga.gov.au'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    "aws_conn_id": "deafrica_data_dev_migration",
    "dest_bucket_name": "africa-migration-test", 
    "schedule_interval": '*/5 * * * *',
    "sqs_queue": "https://sqs.us-west-2.amazonaws.com/565417506782/deafrica-prod-eks-sentinel-2-data-transfer"
}

with DAG('migrate_s3_to_s3', default_args=default_args, schedule_interval=default_args['schedule_interval'], 
        catchup=False, dagrun_timeout=timedelta(seconds=30)) as dag:
   
    process_sqs = SQSSensor(
        task_id='sqs_sensor',
        sqs_queue=dag.default_args['sqs_queue'],
        aws_conn_id=dag.default_args['aws_conn_id'],
        max_messages=10,
        wait_time_seconds=50,
        execution_timeout=timedelta(seconds=20)
    )

    def copy_s3_objects(ti, **kwargs):
        s3_hook = S3Hook(aws_conn_id=dag.default_args['aws_conn_id'])        
        messages = ti.xcom_pull(key='messages', task_ids='sqs_sensor')    

        for m in messages['Messages']:
            body = json.loads(m['Body'])
            message = json.loads(body['Message'])
            src_url = message['assets']['overview']['href']
            src_key = extract_src_key(src_url)
            src_bucket = extract_src_bucket(src_url)
            print("src_bucket_key: ", src_key, "src_bucket: ", src_bucket )              
            s3_hook.copy_object(source_bucket_key=src_key, dest_bucket_key=src_key, 
                               source_bucket_name=src_bucket, dest_bucket_name=default_args['dest_bucket_name'])

    migrate_data = PythonOperator(
        task_id='copy_objects',
        provide_context=True,
        python_callable=copy_s3_objects
    )

    process_sqs >> migrate_data