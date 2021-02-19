"""
# Migrate(copy) data between S3 buckets

DAG to periodically check a SQS and copy new data to Cape Town
The SQS is subscribed to the following SNS topic:
arn:aws:sns:us-west-2:482759440949:cirrus-dev-publish
"""
import json
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import (
    PythonOperator,
    BranchPythonOperator,
)


from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.aws_sqs_sensor import SQSHook
from airflow.contrib.hooks.aws_sns_hook import AwsSnsHook
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook

CONN_ID = "sentinel_2_sync"
US_CONN_ID = "sentinel_2_sync_oregon"

DEST_BUCKET_NAME = "deafrica-sentinel-2-dev-sync"
SRC_BUCKET_NAME = "sentinel-cogs"
SENTINEL2_TOPIC_ARN = (
    "arn:aws:sns:af-south-1:717690029437:sentinel-2-dev-sync-topic"
)
SQS_QUEUE = "deafrica-dev-eks-sentinel-2-sync"
CONCURRENCY = 1

default_args = {
    "owner": "Airflow",
    "email": ["toktam.ebadi@ga.gov.au"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
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


def africa_tile_ids():
    """
    Load Africa tile ids
    :return: Set of tile ids
    """

    africa_tile_ids = set(
        pd.read_csv(
            (
                "https://raw.githubusercontent.com/digitalearthafrica/"
                "deafrica-extent/master/deafrica-mgrs-tiles.csv.gz"
            )
        ).values.ravel()
    )

    return africa_tile_ids


def get_self_link(message_content):
    # Try the first link
    self_link = message_content["links"][0]["href"]
    # But replace it with the canonical one if it exists
    for link in message_content["links"]:
        if link["rel"] == "canonical":
            self_link = link["href"]
    return self_link


def get_derived_from_link(message_content):
    for link in message_content["links"]:
        if link["rel"] == "derived_from":
            return link["href"]


def correct_stac_links(stac_item):
    """
    Replace the https link of the source bucket
    with s3 link of the destination bucket
    """

    # Extract source link before updating STAC file
    src_link = get_self_link(stac_item)

    # Update links to match destination
    stac_str = json.dumps(stac_item)
    # "Replace https with s3 uri"
    stac_item = stac_str.replace(
        "https://sentinel-cogs.s3.us-west-2.amazonaws.com",
        "s3://deafrica-sentinel-2",
    )
    updated_stac = json.loads(stac_item)
    # Update self and derived_from links
    for x in updated_stac["links"]:
        # Replace derived-from link with s3 links to sentinel-cogs bucket
        if x["rel"] == "derived_from":
            x["href"] = src_link.replace(
                "https://sentinel-cogs.s3.us-west-2.amazonaws.com",
                "s3://sentinel-cogs",
            )
        elif x["rel"] == "self":
            x["href"] = src_link.replace(
                "https://sentinel-cogs.s3.us-west-2.amazonaws.com",
                "s3://deafrica-sentinel-2",
            )
    # Drop canonical and via-cirrus links
    updated_stac["links"] = [
        x
        for x in updated_stac["links"]
        if x["rel"] != "canonical" and x["rel"] != "via-cirrus"
    ]
    return updated_stac


def publish_to_sns(updated_stac, attributes):
    """
    Publish a message to a SNS topic
    param updated_stac: STAC with updated links for the destination bucket
    param attributes: Original message attributes
    """

    if "collection" in attributes:
        del attributes["collection"]
    attributes["product"] = "s2_l2a"

    sns_hook = AwsSnsHook(aws_conn_id=CONN_ID)

    "Replace https with s3 uri"
    sns_hook.publish_to_target(
        target_arn=SENTINEL2_TOPIC_ARN,
        message=json.dumps(updated_stac),
        message_attributes=attributes,
    )


def write_scene(src_key):
    """
    Write a file to destination bucket
    param message: key to write
    """

    s3_hook = S3Hook(aws_conn_id=CONN_ID)
    s3_hook.copy_object(
        source_bucket_key=src_key,
        dest_bucket_key=src_key,
        source_bucket_name=SRC_BUCKET_NAME,
        dest_bucket_name=DEST_BUCKET_NAME,
    )
    return True


def start_transfer(stac_item):
    """
    Transfer a scene from source to destination bucket
    """

    s3_hook_oregon = S3Hook(aws_conn_id=US_CONN_ID)
    s3_filepath = get_derived_from_link(stac_item)

    # Check file exists
    bucket_name, key = s3_hook_oregon.parse_s3_url(s3_filepath)
    key_exists = s3_hook_oregon.check_for_key(key, bucket_name=SRC_BUCKET_NAME)
    if not key_exists:
        raise ValueError(
            f"{key} does not exist in the {SRC_BUCKET_NAME} bucket"
        )

    try:
        s3_hook = S3Hook(aws_conn_id=CONN_ID)
        s3_hook.load_string(
            string_data=json.dumps(stac_item),
            key=key,
            bucket_name=DEST_BUCKET_NAME,
        )
    except Exception as exc:
        raise ValueError(f"{key} failed to copy")
    urls = []
    # Add URL of .tif files
    urls.extend(
        [
            v["href"]
            for k, v in stac_item["assets"].items()
            if "geotiff" in v["type"]
        ]
    )

    # Check that all bands and STAC exist
    if len(urls) != 17:
        raise ValueError(
            f"There are less than 17 files in {stac_item.get('id')} scene, failing"
        )

    scene_path = Path(key).parent
    print(f"Copying {scene_path}")

    src_keys = []
    for src_url in urls:
        bucket_name, src_key = s3_hook_oregon.parse_s3_url(src_url)
        key_exists = s3_hook_oregon.check_for_key(
            key, bucket_name=SRC_BUCKET_NAME
        )
        src_keys.append(src_key)

        if not key_exists:
            raise ValueError(
                f"{key} does not exist in the {SRC_BUCKET_NAME} bucket"
            )
    os.environ["AWS_DEFAULT_REGION"] = "af-south-1"
    copied_files = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        task = {executor.submit(write_scene, key): key for key in src_keys}
        for future in as_completed(task):
            scene_to_copy = task[future]
            try:
                result = future.result()
            except Exception as exc:
                raise ValueError(f"{scene_to_copy} failed to copy")
            else:
                copied_files.append(result)

    if len(copied_files) == 17:
        print(f"Succeeded: {scene_path} ")
    else:
        raise ValueError(f"{scene_path} failed to copy")


def is_valid_tile_id(stac_item, valid_tile_ids):

    tile_id = stac_item["id"].split("_")[1]

    if tile_id not in valid_tile_ids:
        print(
            f"{tile_id} is not in the list of Africa tiles for {stac_item.get('id')}"
        )
        return False
    return True


def copy_s3_objects(ti, **kwargs):
    """
    Copy objects from a s3 bucket to another s3 bucket.
    :param ti: Task instance
    """
    successful = 0
    failed = 0

    os.environ["AWS_DEFAULT_REGION"] = "af-south-1"
    sqs_hook = SQSHook(aws_conn_id=CONN_ID)
    sqs = sqs_hook.get_resource_type("sqs")
    queue = sqs.get_queue_by_name(QueueName=SQS_QUEUE)
    messages = get_messages(queue, visibility_timeout=600)
    valid_tile_ids = africa_tile_ids()

    for message in messages:
        message_body = json.loads(message.body)
        stac_item = json.loads(message_body["Message"])
        attributes = message_body.get("MessageAttributes", {})

        try:
            if not is_valid_tile_id(stac_item, valid_tile_ids):
                message.delete()
                continue
            updated_stac = correct_stac_links(stac_item)
            start_transfer(updated_stac)
            publish_to_sns(updated_stac, attributes)
            message.delete()
            successful += 1
            break
        except ValueError as err:
            failed += 1
            print(err)

    ti.xcom_push(key="successful", value=successful)
    ti.xcom_push(key="failed", value=failed)


def trigger_sensor(ti, **kwargs):
    """
    Function to fork tasks
    If there are messages in the queue, it pushes them to task index
    object, and calls the copy function otherwise it calls a task that
    terminates the DAG run
    :param ti: Task instance
    :return: String id of the downstream task
    """

    sqs_hook = SQSHook(aws_conn_id=CONN_ID)
    sqs = sqs_hook.get_resource_type("sqs")
    queue = sqs.get_queue_by_name(QueueName=SQS_QUEUE)
    queue_size = int(queue.attributes.get("ApproximateNumberOfMessages"))
    print("Queue size:", queue_size)

    if queue_size > 0:
        return "run_tasks"
    else:
        return "no_messages__end"


def end_dag():
    print("Message queue is empty, terminating DAG")


def terminate(ti, **kwargs):
    successful_msg_counts = 0
    failed_msg_counts = 0

    for idx in range(CONCURRENCY):
        successful_msg_counts += ti.xcom_pull(
            key="successful", task_ids=f"data_transfer_{idx}"
        )
        failed_msg_counts += ti.xcom_pull(
            key="failed", task_ids=f"data_transfer_{idx}"
        )

    print(
        f"{successful_msg_counts} were successfully processed, and {failed_msg_counts} failed"
    )


with DAG(
    "sentinel-2_data_transfer",
    default_args=default_args,
    tags=["Sentinel-2", "transfer"],
    catchup=False,
    start_date=datetime(2020, 6, 12),
    concurrency=CONCURRENCY,
    schedule_interval="0 */1 * * *",
) as dag:

    BRANCH_OPT = BranchPythonOperator(
        task_id="check_for_messages",
        python_callable=trigger_sensor,
        provide_context=True,
    )

    END_DAG = PythonOperator(
        task_id="no_messages__end", python_callable=end_dag
    )

    TERMINATE_DAG = PythonOperator(
        task_id="terminate", python_callable=terminate, provide_context=True
    )

    RUN_TASKS = DummyOperator(task_id="run_tasks")

    for idx in range(CONCURRENCY):
        COPY_OBJECTS = PythonOperator(
            task_id=f"data_transfer_{idx}",
            provide_context=True,
            retries=1,
            execution_timeout=timedelta(hours=20),
            python_callable=copy_s3_objects,
            task_concurrency=CONCURRENCY,
            dag=dag,
        )
        BRANCH_OPT >> [RUN_TASKS, END_DAG]
        RUN_TASKS >> COPY_OBJECTS >> TERMINATE_DAG
