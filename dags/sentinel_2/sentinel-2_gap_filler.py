"""
# Republish STAC SQS messages from a report of missing S3 objects

This DAG is intended to be run manually, and must be passed a configuration JSON object
specifying `offset`, `limit` line numbers of the report and `s3_report_path` specifying
the path to the gap report file.

Eg:
```json
{
"limit": 1224
}
"""

import gzip
import json
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict

from airflow import DAG
from airflow.contrib.sensors.aws_sqs_sensor import SQSHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator

from infra.connections import CONN_SENTINEL_2_SYNC
from infra.s3_buckets import SENTINEL_2_SYNC_BUCKET_NAME
from infra.sqs_queues import SENTINEL_2_SYNC_SQS_NAME
from infra.variables import REGION
from sentinel_2.variables import REPORTING_PREFIX, SENTINEL_COGS_BUCKET
from utility.utility_slackoperator import task_fail_slack_alert, task_success_slack_alert
from utils.aws_utils import S3

PRODUCT_NAME = "s2_l2a"
SCHEDULE_INTERVAL = None

default_args = {
    "owner": "RODRIGO",
    "start_date": datetime(2020, 7, 24),
    "email": ["systems@digitalearthafrica.org"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "on_failure_callback": task_fail_slack_alert,
}


def get_common_message_attributes(stac_doc: Dict) -> Dict:
    """
    Returns common message attributes dict
    :param stac_doc: STAC dict
    :return: common message attributes dict
    """
    msg_attributes = {
        "product": {
            "Type": "String",
            "Value": PRODUCT_NAME,
        }
    }

    date_time = stac_doc.get("properties").get("datetime")
    if date_time:
        msg_attributes["datetime"] = {
            "Type": "String",
            "Value": date_time,
        }

    cloud_cover = stac_doc.get("properties").get("eo:cloud_cover")
    if cloud_cover:
        msg_attributes["cloudcover"] = {
            "Type": "Number",
            "Value": str(cloud_cover),
        }

    maturity = stac_doc.get("properties").get("dea:dataset_maturity")
    if maturity:
        msg_attributes["maturity"] = {
            "Type": "String",
            "Value": maturity,
        }

    bbox = stac_doc.get("bbox")
    if bbox and len(bbox) > 3:
        msg_attributes["bbox.ll_lon"] = {
            "Type": "Number",
            "Value": str(bbox[0]),
        }
        msg_attributes["bbox.ll_lat"] = {
            "Type": "Number",
            "Value": str(bbox[1]),
        }
        msg_attributes["bbox.ur_lon"] = {
            "Type": "Number",
            "Value": str(bbox[2]),
        }
        msg_attributes["bbox.ur_lat"] = {
            "Type": "Number",
            "Value": str(bbox[3]),
        }

    return msg_attributes


def find_latest_report(update_stac: bool = False) -> str:
    """
    Function to find the latest gap report
    :param update_stac:(bool)
    :return:(str) return the latest report file name
    """
    continuation_token = None
    list_reports = []
    while True:
        s3 = S3(conn_id=CONN_SENTINEL_2_SYNC)

        resp = s3.list_objects(
            bucket_name=SENTINEL_2_SYNC_BUCKET_NAME,
            region=REGION,
            prefix=REPORTING_PREFIX,
            continuation_token=continuation_token,
        )

        if not resp.get("Contents"):
            raise Exception(
                f"Report not found at "
                f"{SENTINEL_2_SYNC_BUCKET_NAME}/{REPORTING_PREFIX}"
                f"  - returned {resp}"
            )

        list_reports.extend(
            [
                obj["Key"]
                for obj in resp["Contents"]
                if "orphaned" not in obj["Key"]
                   and (not update_stac or (update_stac and "update" in obj["key"]))
            ]
        )

        # The S3 API is paginated, returning up to 1000 keys at a time.
        if resp.get("NextContinuationToken"):
            continuation_token = resp["NextContinuationToken"]
        else:
            break

    list_reports.sort()

    if not list_reports:
        raise RuntimeError("Report not found!")

    return list_reports[-1]


def get_missing_stac_files(limit=None):
    """
    read the gap report
    """

    last_report = find_latest_report()

    logging.info(f'This is the last report {last_report}')

    if 'update' in last_report:
        logging.info('FORCED UPDATE FLAGGED!')

    logging.info(f"Limited: {'No limit' if limit is None else int(limit)}")

    s3 = S3(conn_id=CONN_SENTINEL_2_SYNC)

    missing_scene_file_gzip = s3.get_s3_contents_and_attributes(
        bucket_name=SENTINEL_2_SYNC_BUCKET_NAME,
        region=REGION,
        key=last_report,
    )

    missing_scene_paths = [
        scene_path
        for scene_path in gzip.decompress(missing_scene_file_gzip).decode("utf-8").split("\n")
        if scene_path
    ]

    if limit is not None:
        missing_scene_paths = missing_scene_paths[:int(limit)]

    logging.info(f"Number of scenes found {len(missing_scene_paths)}")
    logging.info(f"Example scenes: {missing_scene_paths[0:10]}")

    for f in missing_scene_paths:
        yield f.strip()


def post_messages(messages):
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


def get_contents_and_attributes(hook, s3_filepath):
    bucket_name, key = hook.parse_s3_url(s3_filepath)
    contents = hook.read_key(key=key, bucket_name=SENTINEL_COGS_BUCKET)
    contents_dict = json.loads(contents)

    attributes = get_common_message_attributes(contents_dict)

    return json.dumps(contents_dict), attributes


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
            {
                "Message": contents,
                "MessageAttributes": attributes
            }
        ),
    }
    return message


def publish_message(files):
    """
    """
    hook = S3Hook(aws_conn_id=CONN_SENTINEL_2_SYNC)
    max_workers = 300

    # counter for files that no longer exist
    failed = 0
    sent = 0

    batch = []

    for s3_path in files:
        try:
            batch.append(
                prepare_message(hook, s3_path)
            )
            if len(batch) == 10:
                post_messages(batch)
                batch = []
                sent += 10
        except Exception as exc:
            failed += 1
            logging.info(f"File no longer exists: {exc}")

    if len(batch) > 0:
        post_messages(batch)
        sent += len(batch)
    if failed > 0:
        raise ValueError(f"Total of {failed} files failed, Total of sent messages {sent}")
    logging.info(f"Total of sent messages {sent}")


def prepare_and_send_messages(dag_run, **kwargs) -> None:
    """
    Function to retrieve the latest gap report and create messages to the filter queue process.

    """
    try:

        limit = dag_run.conf.get("limit", None)
        logging.info(f'limit - {limit}')

        files = get_missing_stac_files(limit)

        publish_message(files)

    except Exception as error:
        logging.exception(error)
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
        on_success_callback=task_success_slack_alert,
    )
