"""
# Migrate(copy) data between S3 buckets

DAG to periodically check a SQS and copy new data to Cape Town
The SQS is subscribed to the following SNS topic:
arn:aws:sns:us-west-2:482759440949:cirrus-dev-publish
"""
import json
import boto3
import re
import itertools
import csv
import os
from pathlib import Path
from datetime import datetime, timedelta
import multiprocessing

from airflow import configuration
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.contrib.sensors.aws_sqs_sensor import SQSHook
from airflow.contrib.hooks.aws_sns_hook import AwsSnsHook

from airflow.hooks.S3_hook import S3Hook

default_args = {
    'owner': 'Airflow',
    "start_date": datetime(2020, 6, 12),
    'email': ['toktam.ebadi@ga.gov.au'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'africa_tiles': "data/africa-mgrs-tiles.csv",
    'africa_conn_id': "deafrica-prod-migration",
    "us_conn_id": "deafrica-migration_us",
    "dest_bucket_name": "deafrica-sentinel-2",
    "src_bucket_name": "sentinel-cogs",
    "schedule_interval": "0 */8 * * *",
    "sentinel2_topic_arn": "arn:aws:sns:af-south-1:543785577597:deafrica-sentinel-2-scene-topic",
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

def africa_tile_ids():
    """
    Load Africa tile ids
    :return: Set of tile ids
    """

    tile_ids_filepath = Path(configuration.get('core', 'dags_folder')). \
                        parent.joinpath(default_args['africa_tiles'])
    with open(tile_ids_filepath) as f:
        reader = csv.reader(f)
        list_of_mgrs = [x[0] for x in reader]

    return set(list_of_mgrs)

def publish_to_sns_topic(message, attribute):
    """
    Publish a message to a SNS topic
    param message: message body
    """

    sns_hook = AwsSnsHook(aws_conn_id=dag.default_args['africa_conn_id'])
    response = sns_hook.publish_to_target(target_arn=default_args['sentinel2_topic_arn'],
                                          message=message, message_attributes=attribute)


def copy_scene(args):

    message = args[0]
    attribute = args[1]
    valid_tile_ids = args[2]
    tile_id = message["id"].split("_")[1]

    s3_hook = S3Hook(aws_conn_id=dag.default_args['africa_conn_id'])
    s3_hook_oregon = S3Hook(aws_conn_id=dag.default_args['us_conn_id'])

    if tile_id in valid_tile_ids:
        # Extract URL of the json file
        urls = [message["links"][0]["href"]]
        print(f"Copying {Path(urls[0]).parent}")
        # Add URL of .tif files
        urls.extend([v["href"] for k, v in message["assets"].items() if "geotiff" in v['type']])

        s3_filepath = str(Path(urls[0]).parent)
        bucket, key = s3_filepath.replace(f"s3://{default_args['src_bucket_name']}", "").split("/", 1)
        key_exist = s3_hook_oregon.check_for_prefix(default_args['src_bucket_name'], key, '/')
        if  key_exist is False:
            print(f"{key} does not exist in the {default_args['src_bucket_name']} bucket")
            return

        for src_url in urls:
            src_key = extract_src_key(src_url)
            key_exist = s3_hook_oregon.check_for_prefix(default_args['src_bucket_name'], key, "/")
            if key_exist is False:
                continue
            s3_hook.copy_object(source_bucket_key=src_key,
                                dest_bucket_key=src_key,
                                source_bucket_name=default_args['src_bucket_name'],
                                dest_bucket_name=default_args['dest_bucket_name'])

        publish_to_sns_topic(json.dumps(message), attribute)
        scene = urls[0]
        return Path(Path(scene).name).stem
    print(f"{message["id"]} is outside Africa")

def copy_s3_objects(ti, **kwargs):
    """
    Copy objects from a s3 bucket to another s3 bucket.
    :param ti: Task instance
    """

    messages = ti.xcom_pull(key='Messages', task_ids='test_trigger_dagrun')
    attributes = ti.xcom_pull(key='attributes', task_ids='test_trigger_dagrun')
    # Load Africa tile ids
    valid_tile_ids = africa_tile_ids()
    max_num_cpus = 12
    pool = multiprocessing.Pool(processes=max_num_cpus, maxtasksperchild=2)
    args = [(msg, atr, tile) for msg, atr, tile in zip(messages, attributes, [valid_tile_ids]*len(messages))]
    results = pool.map(copy_scene, args)
    Not_none_values = list(filter(None.__ne__, results))
    print(f"Copied {len(Not_none_values)} out of {len(messages)} files")

def get_queue():
    """
    Return the SQS queue object
    """
    sqs_hook = SQSHook(aws_conn_id=dag.default_args['us_conn_id'])
    queue_url = default_args['sqs_queue']
    queue_name = queue_url[queue_url.rindex("/") + 1:]
    sqs = sqs_hook.get_resource_type('sqs')
    return sqs.get_queue_by_name(QueueName=queue_name)

def trigger_sensor(ti, **kwargs):
    """
    Function to fork tasks
    If there are messages in the queue, it pushes them to task index object, and calls the copy function
    otherwise it calls a task that terminates the DAG run
    :param ti: Task instance
    :return: String id of the downstream task
    """

    queue = get_queue()
    print("Queue size:", int(queue.attributes.get("ApproximateNumberOfMessages")))
    if int(queue.attributes.get("ApproximateNumberOfMessages")) > 0 :
        max_num_polls = 10
        msg_list = [queue.receive_messages(WaitTimeSeconds=5, MaxNumberOfMessages=10) for i in range(max_num_polls)]
        msg_list  = list(itertools.chain(*msg_list))
        messages = []
        attributes = []
        for msg in msg_list:
            body = json.loads(msg.body)
            message = json.loads(body['Message'])
            messages.append(message)
            attributes.append(body['MessageAttributes'])
            print(message['id'])
            msg.delete()
        ti.xcom_push(key="Messages", value=messages)
        ti.xcom_push(key="attributes", value=attributes)
        print(f"Read {len(messages)} messages")
        return "copy_scenes"
    else:
         return "end"

def end_dag():
    print("Message queue is empty, terminating DAG")

with DAG('sentinel-2_data_transfer', default_args=default_args,
         schedule_interval=default_args['schedule_interval'],
         tags=["Sentinel-2", "transfer"], catchup=False) as dag:

    BRANCH_OPT = BranchPythonOperator(
        task_id='test_trigger_dagrun',
        python_callable=trigger_sensor,
        provide_context=True)

    COPY_OBJECTS = PythonOperator(
        task_id='copy_scenes',
        provide_context=True,
        execution_timeout = timedelta(hours=20),
        python_callable=copy_s3_objects
    )

    END_DAG = PythonOperator(
        task_id='end',
        python_callable=end_dag
    )

    BRANCH_OPT >> [COPY_OBJECTS, END_DAG]
