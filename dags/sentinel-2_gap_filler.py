"""
# Republish STAC SQS messages from a report of missing S3 objects

This DAG is intended to be run manually, and must be passed a configuration JSON object
specifying `offset`, `limit` line numbers of the report and `s3_report_path` specifying
the path to the gap report file.

Eg:
```json
{"offset": 824, "limit": 1224, "s3_report_path": "s3://deafrica-sentinel-2/monthly-status-report/2020-11-11T01:38:02.023140.txt"}
"""
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict

from airflow import DAG
from airflow.contrib.sensors.aws_sqs_sensor import SQSHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator

SRC_BUCKET_NAME = "sentinel-cogs"
QUEUE_NAME = "deafrica-prod-eks-sentinel-2-data-transfer"
PRODUCT_NAME = "s2_l2a"

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2020, 7, 24),
    "email": ["toktam.ebadi@ga.gov.au"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "schedule_interval": "@once",
    "us_conn_id": "prod-eks-s2-data-transfer",
    "africa_conn_id": "deafrica-prod-migration",
}


def get_common_message_attributes(stac_doc: Dict) -> Dict:
    """
    Returns common message attributes dict
    :param stac_doc: STAC dict
    :return: common message attributes dict
    """
    msg_attributes = {}
    product = PRODUCT_NAME
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


def get_missing_stac_files(s3_report_path, offset=0, limit=None):
    """
    read the gap report
    """

    hook = S3Hook(aws_conn_id=dag.default_args["africa_conn_id"])
    bucket_name, key = hook.parse_s3_url(s3_report_path)
    print(f"Reading the gap report took {s3_report_path} Seconds")

    files = (
        hook.read_key(key=key, bucket_name=bucket_name)
        .splitlines()
    )

    for f in files[offset:limit]:
        yield f


def publish_messages(messages):
    """
    Publish messages to the data transfer queue
    param message: list of messages
    """

    for num, message in enumerate(messages):
        message["Id"] = str(num)
    sqs_hook = SQSHook(aws_conn_id=dag.default_args["us_conn_id"])
    sqs = sqs_hook.get_resource_type("sqs")
    queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)
    queue.send_messages(Entries=messages)


def get_contents_and_attributes(hook, s3_filepath):
    bucket_name, key = hook.parse_s3_url(s3_filepath)
    contents = hook.read_key(key=key, bucket_name=SRC_BUCKET_NAME)
    contents_dict = json.loads(contents)
    attributes = get_common_message_attributes(contents_dict)
    return contents, attributes


def prepare_message(hook, s3_path):
    """
    Prepare a single message for each stac file
    """

    key_exists = hook.check_for_key(s3_path)
    if not key_exists:
        raise ValueError(f"{s3_path} does not exist")

    contents, attributes = get_contents_and_attributes(hook, s3_path)
    message = {
        "MessageBody": json.dumps(
            {"Message": contents, "MessageAttributes": attributes}
        ),
    }
    return message


def prepare_and_send_messages(dag_run, **kwargs):
    hook = S3Hook(aws_conn_id=dag.default_args["us_conn_id"])
    # Read the missing stac files from the gap report file
    print(f"Reading rows {dag_run.conf['offset']} to {dag_run.conf['limit']} from dag_run.conf["s3_report_path"]")
    files = get_missing_stac_files(
        dag_run.conf["s3_report_path"], dag_run.conf["offset"], dag_run.conf["limit"]
    )

    max_workers = 10
    # counter for files that no longer exist
    failed = 0

    batch = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(prepare_message, hook, s3_path) for s3_path in files]

        for future in as_completed(futures):
            try:
                batch.append(future.result())
                if len(batch) == 10:
                    executor.submit(publish_messages, batch)
                    publish_messages(batch)
                    batch = []
            except Exception as exc:
                failed += 1
                print(f"File no longer exists: {exc}")

    if len(batch) > 0:
        publish_messages(batch)

    print(f"Total of {failed} files failed")


with DAG(
    "sentinel-2-gap-fill",
    default_args=default_args,
    schedule_interval=default_args["schedule_interval"],
    tags=["Sentinel-2", "gap-fill"],
    catchup=False,
    doc_md=__doc__,
) as dag:
    PUBLISH_MESSAGES_FOR_MISSING_SCENES = PythonOperator(
        task_id="publish_messages_for_missing_scenes",
        python_callable=prepare_and_send_messages,
        provide_context=True,
    )
