"""
# Republish STAC SQS messages from a report of missing S3 objects

This DAG is intended to be run manually, and must be passed a configuration JSON object
specifying `offset`, `limit` line numbers of the report and `s3_report_path` specifying
the path to the gap report file.

Eg:
```json
{"offset": 824, "limit": 1224, "s3_report_path": "s3://deafrica-sentinel-2-dev/status-report/2021-08-02T07:39:44.271197.txt.gz"}
"""
import gzip
import json
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict

from airflow import DAG
from airflow.contrib.sensors.aws_sqs_sensor import SQSHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator

from infra.connections import CONN_SENTINEL_2_SYNC
from infra.sqs_queues import SENTINEL_2_SYNC_SQS_NAME
from infra.variables import REGION
from sentinel_2.variables import SENTINEL_COGS_BUCKET
from utils.aws_utils import S3

PRODUCT_NAME = "s2_l2a"
SCHEDULE_INTERVAL = "@once"

default_args = {
    "owner": "RODRIGO",
    "start_date": datetime(2020, 7, 24),
    "email": ["systems@digitalearthafrica.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
}


def get_common_message_attributes(stac_doc: Dict) -> Dict:
    """
    Returns common message attributes dict
    :param stac_doc: STAC dict
    :return: common message attributes dict
    """
    msg_attributes = {
        "product": {
            "DataType": "String",
            "StringValue": PRODUCT_NAME,
        }
    }

    date_time = stac_doc.get("properties").get("datetime")
    if date_time:
        msg_attributes["datetime"] = {
            "DataType": "String",
            "StringValue": date_time,
        }

    cloud_cover = stac_doc.get("properties").get("eo:cloud_cover")
    if cloud_cover:
        msg_attributes["cloudcover"] = {
            "DataType": "Number",
            "StringValue": str(cloud_cover),
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


def get_missing_stac_files(s3_report_path, offset=0, limit=None):
    """
    read the gap report
    """

    print(f"Reading the gap report {s3_report_path}")

    hook = S3Hook(aws_conn_id=CONN_SENTINEL_2_SYNC)
    s3 = S3(conn_id=CONN_SENTINEL_2_SYNC)
    bucket_name, key = hook.parse_s3_url(s3_report_path)

    missing_scene_file_gzip = s3.get_s3_contents_and_attributes(
        bucket_name=bucket_name,
        region=REGION,
        key=key,
    )

    for f in gzip.decompress(missing_scene_file_gzip).decode("utf-8").split("\n")[offset:limit]:
        yield f.strip()


def publish_messages(messages):
    """
    Publish messages to the data transfer queue
    param message: list of messages
    """

    for num, message in enumerate(messages):
        message["Id"] = str(num)
    sqs_hook = SQSHook(aws_conn_id=CONN_SENTINEL_2_SYNC)
    sqs = sqs_hook.get_resource_type("sqs")
    queue = sqs.get_queue_by_name(QueueName=SENTINEL_2_SYNC_SQS_NAME)
    queue.send_messages(Entries=messages)


def get_contents_and_attributes(hook, s3_filepath, update_stac: bool = False):
    bucket_name, key = hook.parse_s3_url(s3_filepath)
    contents = hook.read_key(key=key, bucket_name=SENTINEL_COGS_BUCKET)
    contents_dict = json.loads(contents)
    contents_dict.update(
        {
            "update_stac": update_stac
        }
    )
    attributes = get_common_message_attributes(contents_dict)
    return json.dumps(contents_dict), attributes


def prepare_message(hook, s3_path, update_stac: bool = False):
    """
    Prepare a single message for each stac file
    """

    key_exists = hook.check_for_key(s3_path)
    if not key_exists:
        raise ValueError(f"{s3_path} does not exist")

    contents, attributes = get_contents_and_attributes(hook, s3_path, update_stac)
    message = {
        "MessageBody": json.dumps(
            {
                "Message": contents,
                "MessageAttributes": attributes
            }
        ),
    }
    return message


def prepare_and_send_messages(dag_run, **kwargs) -> None:
    """
    Function to retrieve the latest gap report and create messages to the filter queue process.

    """

    try:
        hook = S3Hook(aws_conn_id=CONN_SENTINEL_2_SYNC)
        # Read the missing stac files from the gap report file
        print(
            f"Reading rows {dag_run.conf['offset']} to {dag_run.conf['limit']} from {dag_run.conf['s3_report_path']}"
        )

        update_stac = False
        if 'update' in dag_run.conf["s3_report_path"]:
            print('FORCED UPDATE FLAGGED!')
            update_stac = True

        files = get_missing_stac_files(
            dag_run.conf["s3_report_path"],
            dag_run.conf["offset"],
            dag_run.conf["limit"],
        )

        max_workers = 10
        # counter for files that no longer exist
        failed = 0

        batch = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(prepare_message, hook, s3_path, update_stac)
                for s3_path in files
            ]

            for future in as_completed(futures):
                try:
                    batch.append(future.result())
                    if len(batch) == 10:
                        print(f'sending 10 messages {batch}')
                        publish_messages(batch)
                        batch = []
                except Exception as exc:
                    failed += 1
                    print(f"File no longer exists: {exc}")

        if len(batch) > 0:
            print(f'sending last messages {batch}')
            publish_messages(batch)

        print('Messages sent')
        print(f"Total of {failed} files failed")
    except Exception as error:
        print(error)
        # print traceback but does not stop execution
        traceback.print_exc()
        raise error


with DAG(
    "sentinel-2-gap-filler",
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
    tags=["Sentinel-2", "gap-fill"],
    catchup=False,
    doc_md=__doc__,
) as dag:
    PUBLISH_MESSAGES_FOR_MISSING_SCENES = PythonOperator(
        task_id="publish_messages_for_missing_scenes",
        python_callable=prepare_and_send_messages,
        provide_context=True,
    )
