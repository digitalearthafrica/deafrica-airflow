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
from typing import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

from airflow import configuration
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.aws_sqs_sensor import SQSHook
from airflow.contrib.hooks.aws_sns_hook import AwsSnsHook
from airflow.hooks.S3_hook import S3Hook

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2020, 6, 12),
    "email": ["toktam.ebadi@ga.gov.au"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "num_workers": 10,
    "africa_conn_id": "deafrica-prod-migration",
    "us_conn_id": "deafrica-migration_us",
    "dest_bucket_name": "deafrica-sentinel-2",
    "src_bucket_name": "sentinel-cogs",
    "schedule_interval": "@once",
    "sentinel2_topic_arn": "arn:aws:sns:af-south-1:543785577597:deafrica-sentinel-2-scene-topic",
    "sqs_queue": "deafrica-prod-eks-sentinel-2-data-transfer",
}


def get_messages(
    queue,
    limit: bool = None,
    visibility_timeout: int = 60,
    message_attributes: Iterable[str] = ["All"],
):
    """
    Get messages from a queue resource.
    """
    count = 0
    while True:
        messages = queue.receive_messages(
            VisibilityTimeout=visibility_timeout,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
            MessageAttributeNames=message_attributes,
        )
        if len(messages) == 0 or (limit and count >= limit):
            break
        else:
            for message in messages:
                count += 1
                yield message


def extract_src_key(src_url):
    """
    Extract object key from specified url path e.g.
    https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/
    2020/S2A_38PKS_20200609_0_L2A/B11.tif
    :param src_url: Full http path of the object
    :return: Object path relative to the base bucket
    """

    matches = re.finditer("/", src_url)
    matches_positions = [match.start() for match in matches]
    start = matches_positions[2] + 1
    return src_url[start:]


def africa_tile_ids():
    """
    Load Africa tile ids
    :return: Set of tile ids
    """

    africa_tile_ids = set(
        pd.read_csv(
            "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/deafrica-mgrs-tiles.csv.gz"
        ).values.ravel()
    )

    return africa_tile_ids


def publish_to_sns_topic(message):
    """
    Publish a message to a SNS topic
    param message: message body
    """

    body = json.loads(message.body)
    attributes = body.get("MessageAttributes", "")
    metadata = json.loads(body.get("Message"))
    metadata = json.dumps(metadata)

    sns_hook = AwsSnsHook(aws_conn_id=dag.default_args["africa_conn_id"])
    "Replace https with s3 uri"
    metadata = metadata.replace(
        "https://sentinel-cogs.s3.us-west-2.amazonaws.com", "s3://deafrica-sentinel-2"
    )
    response = sns_hook.publish_to_target(
        target_arn=default_args["sentinel2_topic_arn"],
        message=metadata,
        message_attributes=attributes,
    )


def write_scene(key):
    """
    Write a file to destination bucket
    param message: key to write
    """
    s3_hook = S3Hook(aws_conn_id=dag.default_args["africa_conn_id"])
    s3_hook.copy_object(
        source_bucket_key=src_key,
        dest_bucket_key=src_key,
        source_bucket_name=default_args["src_bucket_name"],
        dest_bucket_name=default_args["dest_bucket_name"],
    )
    return "Done"


def start_transfer(message, valid_tile_ids):
    """
    Transfer a scene from source to destination bucket
    """
    body = json.loads(message.body)
    metadata = json.loads(body["Message"])
    tile_id = metadata["id"].split("_")[1]

    s3_hook = S3Hook(aws_conn_id=dag.default_args["africa_conn_id"])
    s3_hook_oregon = S3Hook(aws_conn_id=dag.default_args["us_conn_id"])

    if not tile_id in valid_tile_ids:
        print(f"{tile_id} is not in the list of Africa tiles for {metadata.get('id')}")
        message.delete()

    # Extract URL of the json file
    urls = [metadata["links"][0]["href"]]
    print(f"Copying {Path(urls[0]).parent}")
    # Add URL of .tif files
    urls.extend(
        [v["href"] for k, v in metadata["assets"].items() if "geotiff" in v["type"]]
    )

    s3_filepath = str(Path(urls[0]))
    if "s3:/" in s3_filepath:
        key = s3_filepath.replace(f"s3:/{default_args['src_bucket_name']}/", "").split(
            "/", 0
        )[0]
    else:
        key = s3_filepath.replace(
            f"https:/{default_args['src_bucket_name']}.s3.us-west-2.amazonaws.com/",
            "",
        ).split("/", 0)[0]
    key_exists = s3_hook_oregon.check_for_key(
        key, bucket_name=default_args["src_bucket_name"]
    )
    if not key_exists:
        print(f"{key} does not exist in the {default_args['src_bucket_name']} bucket")
        message.delete()

    # Check that all bands and STAC exist
    if len(urls) != 18:
        raise ValueError(
            f"There are less than 18 files in {metadata.get('id')} scene, failing"
        )

    src_keys = []
    for src_url in urls:
        src_key = extract_src_key(src_url)
        key_exists = s3_hook_oregon.check_for_key(
            key, bucket_name=default_args["src_bucket_name"]
        )
        src_keys.append(src_key)

        if not key_exists:
            raise ValueError(
                f"{key} does not exist in the {default_args['src_bucket_name']} bucket"
            )

    with ThreadPoolExecutor(max_workers=5) as executor:
        task = {executor.submit(write_scene, key): key for key in src_keys}
        for future in as_completed(task):
            scene_to_copy = task[future]
            try:
                result = future.result()
            except Exception as exc:
                raise ValueError(f"{scene_to_copy} failed to copy")
            else:
                print(f"Task {scene_to_copy} succeded with {result}")


def copy_s3_objects(ti, **kwargs):
    """
    Copy objects from a s3 bucket to another s3 bucket.
    :param ti: Task instance
    """
    successful = 0
    failed = 0

    sqs_hook = SQSHook(aws_conn_id=dag.default_args["us_conn_id"])
    sqs = sqs_hook.get_resource_type("sqs")
    queue = sqs.get_queue_by_name(QueueName=default_args.get("sqs_queue"))
    messages = get_messages(queue, visibility_timeout=600)
    valid_tile_ids = africa_tile_ids()
    for message in messages:
        try:
            start_transfer(message, valid_tile_ids)
            publish_to_sns_topic(message)
            message.delete()
            successful += 1
        except ValueError as err:
            failed += 1
            print(err)

    ti.xcom_push(key="successful", value=successful)
    ti.xcom_push(key="failed", value=failed)


def trigger_sensor(ti, **kwargs):
    """
    Function to fork tasks
    If there are messages in the queue, it pushes them to task index object, and calls the copy function
    otherwise it calls a task that terminates the DAG run
    :param ti: Task instance
    :return: String id of the downstream task
    """

    sqs_hook = SQSHook(aws_conn_id=dag.default_args["us_conn_id"])
    sqs = sqs_hook.get_resource_type("sqs")
    queue = sqs.get_queue_by_name(QueueName=default_args.get("sqs_queue"))
    messages = get_messages(queue, visibility_timeout=600)
    messages = list(itertools.islice(messages, 1))
    if len(messages) > 0:
        return "run_tasks"
    if messages:
        return "run_tasks"
    else:
        return "no_messages__end"


def end_dag():
    print("Message queue is empty, terminating DAG")


def terminate(ti, **kwargs):
    successful_msg_counts = 0
    failed_msg_counts = 0

    for idx in range(0, default_args["num_workers"]):
        successful_msg_counts += ti.xcom_pull(
            key="successful", task_ids=f"data_transfer_{idx}"
        )
        failed_msg_counts += ti.xcom_pull(key="failed", task_ids=f"data_transfer_{idx}")

    print(
        f"{successful_msg_counts} were successfully processed, and {failed_msg_counts} failed"
    )


with DAG(
    "sentinel-2_data_transfer",
    default_args=default_args,
    schedule_interval=default_args["schedule_interval"],
    tags=["Sentinel-2", "transfer"],
    catchup=False,
) as dag:

    BRANCH_OPT = BranchPythonOperator(
        task_id="check_for_messages",
        python_callable=trigger_sensor,
        provide_context=True,
    )

    END_DAG = PythonOperator(task_id="no_messages__end", python_callable=end_dag)

    TERMINATE_DAG = PythonOperator(
        task_id="terminate", python_callable=terminate, provide_context=True
    )

    RUN_TASKS = DummyOperator(task_id="run_tasks")

    for idx in range(0, default_args["num_workers"]):
        COPY_OBJECTS = PythonOperator(
            task_id=f"data_transfer_{idx}",
            provide_context=True,
            retries=1,
            execution_timeout=timedelta(hours=20),
            python_callable=copy_s3_objects,
        )
        BRANCH_OPT >> [RUN_TASKS, END_DAG]
        RUN_TASKS >> COPY_OBJECTS >> TERMINATE_DAG
