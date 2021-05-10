"""
# Migrate(copy) data between S3 buckets
DAG to periodically check a SQS and copy new data to Cape Town
The SQS is subscribed to the following SNS topic:
arn:aws:sns:us-west-2:482759440949:cirrus-dev-publish
"""
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

from airflow import DAG
from airflow.contrib.hooks.aws_sns_hook import AwsSnsHook
from airflow.contrib.sensors.aws_sqs_sensor import SQSHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import (
    PythonOperator,
    BranchPythonOperator,
)
from pystac import Item, Link

from infra.connections import S2_AFRICA_CONN_ID, S2_US_CONN_ID
from infra.s3_buckets import (
    SENTINEL_2_SYNC_BUCKET_NAME,
    SENTINEL_2_INVENTORY_BUCKET_NAME,
)
from infra.sns_topics import SYNC_SENTINEL_2_CONNECTION_TOPIC_ARN
from infra.sqs_queues import SENTINEL_2_SYNC_SQS_QUEUE
from infra.variables import AWS_DEFAULT_REGION
from sentinel_2.variables import AFRICA_TILES, SENTINEL_COGS_BUCKET, SENTINEL_2_URL
from utils.aws_utils import SQS
from utils.sync_utils import read_csv_from_gzip

CONCURRENCY = 32

default_args = {
    "owner": "Airflow",
    "email": ["alex.leith@ga.gov.au", "rodrigo.carvalho@ga.gov.au"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
}


def get_messages(
    limit: int = None,
    visibility_timeout: int = 60,
):
    """
    Get messages from a queue resource.
    """
    sqs_queue = SQS(S2_US_CONN_ID, AWS_DEFAULT_REGION)
    count = 0
    while True:
        messages = sqs_queue.receive_messages(
            queue_name=SENTINEL_2_SYNC_SQS_QUEUE,
            visibility_timeout=visibility_timeout,
            max_number_messages=1,
            wait_time_seconds=10,
        )
        if len(messages) == 0 or (limit and count >= limit):
            break
        else:
            for message in messages:
                count += 1
                yield message


def correct_stac_links(stac_item: Item):
    """
    Replace the https link of the source bucket
    with s3 link of the destination bucket
    """

    # Replace self link for canonical link
    self_link = stac_item.get_single_link("self")
    canonical_link = stac_item.get_single_link("canonical")
    if self_link and canonical_link:
        self_link.target = canonical_link.target
    elif not self_link and canonical_link:
        stac_item.add_link(
            Link(
                rel="self",
                target=canonical_link.target,
            )
        )

    # Update derived_from link to match destination
    derived_from_link = stac_item.get_single_link("derived_from")
    derived_from_link.target = self_link.get_href().replace(
        SENTINEL_2_URL,
        f"s3://{SENTINEL_COGS_BUCKET}",
    )

    # Update self link to match destination
    self_link = stac_item.get_single_link("self")
    self_link.target = self_link.get_href().replace(
        SENTINEL_2_URL,
        f"s3://{SENTINEL_2_INVENTORY_BUCKET_NAME}",
    )

    stac_item.links.remove(stac_item.get_single_link("canonical"))
    stac_item.links.remove(stac_item.get_single_link("via-cirrus"))

    logging.info(f"Links changed to {[link for link in stac_item.links]}")

    return stac_item


def publish_to_sns(updated_stac: Item, attributes):
    """
    Publish a message to a SNS topic
    param updated_stac: STAC with updated links for the destination bucket
    param attributes: Original message attributes
    """

    if "collection" in attributes:
        del attributes["collection"]
    attributes["product"] = "s2_l2a"

    sns_hook = AwsSnsHook(aws_conn_id=S2_AFRICA_CONN_ID)

    sns_hook.publish_to_target(
        target_arn=SYNC_SENTINEL_2_CONNECTION_TOPIC_ARN,
        message=json.dumps(updated_stac.to_dict()),
        message_attributes=attributes,
    )


def write_scene(src_key):
    """
    Write a file to destination bucket
    param message: key to write
    """
    s3_hook = S3Hook(aws_conn_id=S2_AFRICA_CONN_ID)
    s3_hook.copy_object(
        source_bucket_key=src_key,
        dest_bucket_key=src_key,
        source_bucket_name=SENTINEL_COGS_BUCKET,
        dest_bucket_name=SENTINEL_2_SYNC_BUCKET_NAME,
    )
    return True


def start_transfer(stac_item: Item):
    """
    Transfer a scene from source to destination bucket
    """
    logging.info("Transfering Files")

    s3_hook_oregon = S3Hook(aws_conn_id=S2_US_CONN_ID)
    derived_from_link = stac_item.get_single_link("derived_from")
    s3_filepath = derived_from_link.get_href()

    # Check file exists
    logging.info(f"Check if file exist {s3_filepath}")
    bucket_name, stac_key = s3_hook_oregon.parse_s3_url(s3_filepath)
    key_exists = s3_hook_oregon.check_for_key(
        key=stac_key, bucket_name=SENTINEL_COGS_BUCKET
    )
    if not key_exists:
        raise ValueError(
            f"{stac_key} does not exist in the {SENTINEL_COGS_BUCKET} bucket"
        )

    # Add URL of .tif files
    urls = [
        asset.href
        for k, asset in stac_item.get_assets().items()
        if "geotiff" in asset.media_type
    ]

    # Check that all bands and STAC exist
    if len(urls) != 17:
        raise ValueError(
            f"There are less than 17 files in {stac_item.id} scene, failing"
        )

    scene_path = Path(stac_key).parent
    logging.info(f"Copying {scene_path}")

    src_keys = []
    for src_url in urls:
        bucket_name, src_key = s3_hook_oregon.parse_s3_url(src_url)
        key_exists = s3_hook_oregon.check_for_key(
            src_key, bucket_name=SENTINEL_COGS_BUCKET
        )
        src_keys.append(src_key)

        if not key_exists:
            raise ValueError(
                f"{src_key} does not exist in the {SENTINEL_COGS_BUCKET} bucket"
            )

    logging.info(f"These are keys {src_keys}")
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

    # write the STAC file to s3
    if len(copied_files) == 17:
        logging.info(f"Succeeded: {scene_path} ")
    else:
        raise ValueError(f"{scene_path} failed to copy")
    try:
        s3_hook = S3Hook(aws_conn_id=S2_AFRICA_CONN_ID)
        s3_hook.load_string(
            string_data=json.dumps(stac_item.to_dict()),
            key=stac_key,
            replace=True,
            bucket_name=SENTINEL_2_SYNC_BUCKET_NAME,
        )
    except Exception as exc:
        raise ValueError(f"{stac_key} failed to copy")


def is_valid_tile_id(stac_item: Item, valid_tile_ids: list):
    """

    :param stac_item:
    :param valid_tile_ids:
    :return:
    """
    tile_id = stac_item.id.split("_")[1]

    if not tile_id or tile_id not in valid_tile_ids:
        logging.error(
            f"{tile_id} is not in the list of Africa tiles for {stac_item.id}"
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

    logging.info(f"Connecting to AWS SQS {SENTINEL_2_SYNC_SQS_QUEUE}")
    logging.info(f"Conn_id Name {S2_US_CONN_ID}")
    messages = get_messages(limit=20, visibility_timeout=600)

    logging.info("Reading Africa's visible tiles")
    valid_tile_ids = read_csv_from_gzip(file_path=AFRICA_TILES)

    for message in messages:
        message_body = json.loads(message.body)
        message_body_dict = json.loads(message_body["Message"])

        logging.info(f"Message received {message_body_dict}")
        logging.info(f"Converting message into pystac Item")
        stac_item = Item.from_dict(message_body_dict)

        attributes = message_body.get("MessageAttributes", {})

        try:
            if not is_valid_tile_id(stac_item, valid_tile_ids):
                message.delete()
                continue

            logging.info("Correcting links")
            updated_stac = correct_stac_links(stac_item)
            logging.info("Corrected")

            start_transfer(updated_stac)
            publish_to_sns(updated_stac, attributes)
            # message.delete()
            successful += 1
        except ValueError as err:
            failed += 1
            logging.error(err)
        logging.info(
            "*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*"
        )
        logging.info("")

    logging.info(
        "====================================================================="
    )
    ti.xcom_push(key="successful", value=successful)
    ti.xcom_push(key="failed", value=failed)
    logging.info(
        "====================================================================="
    )


def trigger_sensor(ti, **kwargs):
    """
    Function to fork tasks
    If there are messages in the queue, it pushes them to task index
    object, and calls the copy function otherwise it calls a task that
    terminates the DAG run
    :param ti: Task instance
    :return: String id of the downstream task
    """

    sqs_hook = SQSHook(aws_conn_id=S2_US_CONN_ID)
    sqs = sqs_hook.get_resource_type("sqs")
    queue = sqs.get_queue_by_name(QueueName=SENTINEL_2_SYNC_SQS_QUEUE)
    queue_size = int(queue.attributes.get("ApproximateNumberOfMessages"))
    logging.info(f"Queue size: {queue_size}")

    if queue_size > 0:
        return "run_tasks"
    else:
        return "no_messages__end"


def end_dag():
    """

    :return:
    """
    logging.info("Message queue is empty, terminating DAG")


def terminate(ti, **kwargs):
    """

    :param ti:
    :param kwargs:
    :return:
    """
    successful_msg_counts = 0
    failed_msg_counts = 0

    for idx in range(CONCURRENCY):
        successful_msg_counts += ti.xcom_pull(
            key="successful", task_ids=f"data_transfer_{idx}"
        )
        failed_msg_counts += ti.xcom_pull(key="failed", task_ids=f"data_transfer_{idx}")

    logging.info(
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

    END_DAG = PythonOperator(task_id="no_messages__end", python_callable=end_dag)

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
