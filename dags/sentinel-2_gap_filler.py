import csv
import json
import sys
import time
import boto3
from datetime import datetime
from pathlib import Path
from typing import Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow import DAG, configuration
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_sns_hook import AwsSnsHook
from airflow.contrib.sensors.aws_sqs_sensor import SQSHook
from airflow.operators.python_operator import PythonOperator

from utils.inventory import s3

OFFSET = 824
LIMIT = 1224

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2020, 7, 24),
    "email": ["toktam.ebadi@ga.gov.au"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    # "s3_report_path": "s3://deafrica-sentinel-2/monthly-status-report/2020-11-11T01:38:02.023140.txt",
    "report_bucket": "deafrica-sentinel-2",
    "schedule_interval": "@once",
    "us_conn_id": "prod-eks-s2-data-transfer",
    "africa_conn_id": "deafrica-prod-migration",
    "src_bucket_name": "sentinel-cogs",
    "queue_name": "deafrica-prod-eks-sentinel-2-data-transfer",
    "product_name": "s2_l2a",
}


def get_common_message_attributes(stac_doc: Dict) -> Dict:
    """
    Returns common message attributes dict
    :param stac_doc: STAC dict
    :return: common message attributes dict
    """
    msg_attributes = {}
    product = default_args["product_name"]
    msg_attributes["product"] = {
        "DataType": "String",
        "StringValue": product,
    }

    datetime = stac_doc.get("properties").get("datetime")
    if datetime:
        msg_attributes["datetime"] = {
            "DataType": "String",
            "StringValue": datetime,
        }

    cloudcover = stac_doc.get("properties").get("eo:cloud_cover")
    if cloudcover:
        msg_attributes["cloudcover"] = {
            "DataType": "Number",
            "StringValue": str(cloudcover),
        }

    maturity = stac_doc.get("properties").get("dea:dataset_maturity")
    if maturity:
        msg_attributes["maturity"] = {
            "DataType": "String",
            "StringValue": maturity,
        }

    bbox = stac_doc.get("bbox")
    if bbox and len(bbox) > 3:
        msg_attributes["bbox.ll_lon"] = {
            "DataType": "Number",
            "StringValue": str(bbox[0]),
        }
        msg_attributes["bbox.ll_lat"] = {
            "DataType": "Number",
            "StringValue": str(bbox[1]),
        }
        msg_attributes["bbox.ur_lon"] = {
            "DataType": "Number",
            "StringValue": str(bbox[2]),
        }
        msg_attributes["bbox.ur_lat"] = {
            "DataType": "Number",
            "StringValue": str(bbox[3]),
        }

    return msg_attributes


def get_missing_stac_files(offset=0, limit=None):
    """
    read the gap report
    """

    hook = S3Hook(aws_conn_id=dag.default_args["africa_conn_id"])
    bucket_name, key = hook.parse_s3_url(default_args["s3_report_path"])
    print(f"Reading the gap report took {default_args['s3_report_path']} Seconds")

    # ToDo: changing the bucket name was due to a bug. Remove this when new data is available.
    files = (
        hook.read_key(key=key, bucket_name=bucket_name)
        .replace("sentinel-cogs-inventory", f"{default_args['src_bucket_name']}")
        .splitlines()
    )

    for f in files[offset:limit]:
        yield f


def publish_messages(messages):
    """
    Publish messages to the data transfer queue
    param message: list of messages
    """

    sqs_hook = SQSHook(aws_conn_id=dag.default_args["us_conn_id"])
    sqs = sqs_hook.get_resource_type("sqs")
    queue = sqs.get_queue_by_name(QueueName=default_args.get("queue_name"))
    queue.send_messages(Entries=messages)
    return []


def get_contents_and_attributes(hook, s3_filepath):
    bucket_name, key = hook.parse_s3_url(s3_filepath)
    contents = hook.read_key(key=key, bucket_name=default_args["src_bucket_name"])
    contents_dict = json.loads(contents)
    attributes = get_common_message_attributes(contents_dict)
    return contents, attributes


def prepare_message(hook, s3_path, uid):
    """
    Prepare a single message for each stac file
    """

    key_exists = hook.check_for_key(s3_path)
    if not key_exists:
        raise ValueError(f"{s3_path} does not exist")

    contents, attributes = get_contents_and_attributes(hook, s3_path)
    message = {
        "Id": str(uid),
        "MessageBody": json.dumps(
            {"Message": contents, "MessageAttributes": attributes}
        ),
    }
    return message


def prepare_and_send_messages():

    hook = S3Hook(aws_conn_id=dag.default_args["us_conn_id"])
    # Read the missing stac files from the gap report file
    files = get_missing_stac_files(OFFSET, LIMIT)

    max_workers = 10
    counter = 0
    to_process = []
    messages = []
    # counter for files that no longer exist
    failed = 0

    for s3_path in files:
        to_process.append(s3_path)

        if len(to_process) == 10:

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                uids = [
                    x
                    for x in range(
                        max_workers * counter, max_workers * counter + max_workers
                    )
                ]
                task = [
                    executor.submit(prepare_message, hook, key, uid)
                    for key, uid in zip(to_process, uids)
                ]

                for future in as_completed(task):
                    try:
                        result = future.result()
                    except Exception as exc:
                        failed += 1
                    else:
                        messages.append(result)

            if len(messages) > 0:
                messages = publish_messages(messages)
                to_process = []
                counter += 1

    print(f"Total of {failed} files failed")


with DAG(
    "sentinel-2-gap-fill",
    default_args=default_args,
    schedule_interval=default_args["schedule_interval"],
    tags=["Sentinel-2", "gap-fill"],
    catchup=False,
) as dag:

    PUBLISH_MESSAGES_FOR_MISSING_SCENES = PythonOperator(
        task_id="publish_messages_for_missing_scenes",
        python_callable=prepare_and_send_messages,
    )

    PUBLISH_MESSAGES_FOR_MISSING_SCENES
