"""
# Migrate(copy) data between S3 buckets

DAG to periodically check SQS and copy new data to Cape Town
In case where the queue is empty, a timeout policy is applied to kill the DAG
"""

from datetime import datetime, timedelta
from airflow import DAG, configuration
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.aws_sqs_sensor import SQSSensor

import json
import logging

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
    "sqs_queue": "https://sqs.us-west-2.amazonaws.com/565417506782/test_africa"
}

with DAG('migrate_s3_to_s3', default_args=default_args, schedule_interval=default_args['schedule_interval'], 
        catchup=False, dagrun_timeout=timedelta(seconds=30)) as dag:
   
    process_sqs = SQSSensor(
        task_id='sqs_sensor',
        sqs_queue=dag.default_args['sqs_queue'],
        aws_conn_id=dag.default_args['aws_conn_id'],
        max_messages=10,
        wait_time_seconds=5,
        execution_timeout=timedelta(seconds=20)
    )

    def copy_s3_objects(ti, **kwargs):
        s3_hook = S3Hook(aws_conn_id=dag.default_args['aws_conn_id'])        
        messages = ti.xcom_pull(key='messages', task_ids='sqs_sensor')         
        for m in messages['Messages']:
            body = json.loads(m['Body'])
            for rec in body['Records']:
                src_key = rec['s3']['object']['key']
                src_bucket = rec['s3']['bucket']['name']                
                s3_hook.copy_object(source_bucket_key=src_key, dest_bucket_key=src_key, 
                                        source_bucket_name=src_bucket, dest_bucket_name=default_args['dest_bucket_name'])

    migrate_data = PythonOperator(
        task_id='copy_objects',
        provide_context=True,
        python_callable=copy_s3_objects
    )

    process_sqs >> migrate_data