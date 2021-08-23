"""
# Republish STAC SQS messages from a report of missing S3 objects

This DAG is intended to be run manually, and must be passed a configuration JSON object
specifying `offset`, `limit` line numbers of the report and `s3_report_path` specifying
the path to the gap report file.

Eg:
```json
{
"limit": 1234
}
"""

import gzip
import json
import logging
import math
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict
from urllib.parse import urlparse

from airflow import DAG
from airflow.contrib.sensors.aws_sqs_sensor import SQSHook
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowSkipException

from infra.connections import CONN_SENTINEL_2_SYNC
from infra.s3_buckets import SENTINEL_2_SYNC_BUCKET_NAME
from infra.sqs_queues import SENTINEL_2_SYNC_SQS_NAME
from infra.variables import REGION
from sentinel_2.variables import REPORTING_PREFIX, SENTINEL_COGS_AWS_REGION
from utility.utility_slackoperator import task_fail_slack_alert, task_success_slack_alert
from utils.aws_utils import S3

PRODUCT_NAME = "s2_l2a"
SCHEDULE_INTERVAL = None
NUN_WORKERS = 30

DEFAULT_ARGS = {
    "owner": "RODRIGO",
    "start_date": datetime(2020, 7, 24),
    "email": ["systems@digitalearthafrica.org"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "on_failure_callback": task_fail_slack_alert,
    "on_success_callback": task_success_slack_alert,
}


def parse_s3_url(s3url):
    parsed_url = urlparse(s3url)
    if not parsed_url.netloc:
        raise Exception(f'Please provide a bucket_name instead of {s3url}')
    else:
        bucket_name = parsed_url.netloc
        key = parsed_url.path.strip('/')
        return bucket_name, key


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


def get_missing_stac_files(limit=None, **context):
    """
    read the gap report
    """
    logging.info(f'limit - {limit}')

    last_report = find_latest_report()

    logging.info(f'This is the last report {last_report}')

    if 'update' in last_report:
        logging.info('FORCED UPDATE FLAGGED!')

    logging.info(f"Limited: {'No limit' if limit else int(limit)}")

    s3 = S3(conn_id=CONN_SENTINEL_2_SYNC)

    missing_scene_file_gzip = s3.get_s3_contents_and_attributes(
        bucket_name=SENTINEL_2_SYNC_BUCKET_NAME,
        region=REGION,
        key=last_report,
    )

    missing_scene_paths = [
        scene_path.strip()
        for scene_path in gzip.decompress(missing_scene_file_gzip).decode("utf-8").split("\n")
        if scene_path
    ]

    if limit:
        missing_scene_paths = missing_scene_paths[:int(limit)]

    logging.info(f"Number of scenes found {len(missing_scene_paths)}")
    logging.info(f"Example scenes: {missing_scene_paths[0:10]}")

    return missing_scene_paths


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


def get_contents_and_attributes(s3_filepath):
    s3 = S3(conn_id=CONN_SENTINEL_2_SYNC)
    bucket_name, key = parse_s3_url(s3_filepath)

    contents = s3.get_object(
        bucket_name=bucket_name,
        key=str(key),
        region=SENTINEL_COGS_AWS_REGION,
    ).get("Body").read()
    contents_dict = json.loads(contents)

    attributes = get_common_message_attributes(contents_dict)

    return json.dumps(contents_dict), attributes


def prepare_message(s3_path):
    """
    Prepare a single message for each stac file
    """
    s3 = S3(conn_id=CONN_SENTINEL_2_SYNC)

    bucket_name, key = parse_s3_url(s3_path)
    key_exists = s3.check_for_key(
        bucket_name=bucket_name,
        region=SENTINEL_COGS_AWS_REGION,
        key=key,
    )

    if not key_exists:
        raise ValueError(f"{s3_path} does not exist")

    contents, attributes = get_contents_and_attributes(s3_path)
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
    max_workers = 300
    # counter for files that no longer exist
    failed = 0
    sent = 0

    batch = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(prepare_message, s3_path)
            for s3_path in files
        ]

        for future in as_completed(futures):
            try:
                batch.append(future.result())
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


def prepare_and_send_messages(
        xcom_task_id: str,
        idx,
        **context
) -> None:
    """
    Function to retrieve the latest gap report and create messages to the filter queue process.

    """
    try:
        files = context['ti'].xcom_pull(task_ids=xcom_task_id)
        logging.info(f"IDX {idx}")

        # This code will create chunks of 50 scenes and run over the workers
        max_list_items = math.ceil(len(files) / NUN_WORKERS)
        if max_list_items < 1:
            raise Exception('Files not found.')

        if idx >= max_list_items:
            logging.info("Got into the exception")
            raise AirflowSkipException

        iter_list = [
            files[i:i + max_list_items]
            for i in range(0, len(files), max_list_items)
        ][idx]

        logging.info(f"Number of files to be sent {len(iter_list)}")
        logging.info(f"10 First files {[f for f in iter_list][:10]}")
        publish_message(iter_list)

    except Exception as error:
        logging.exception(error)
        # print traceback but does not stop execution
        traceback.print_exc()
        raise error


GET_SCENES_TASK_NAME = "gap_scenes"
DAG_NAME = "sentinel-2-gap-filler-2"
with DAG(
        DAG_NAME,
        default_args=DEFAULT_ARGS,
        schedule_interval=SCHEDULE_INTERVAL,
        tags=["Sentinel-2", "gap-fill"],
        catchup=False,
        doc_md=__doc__,
        max_active_runs=NUN_WORKERS
) as dag:
    PUBLISH_MISSING_SCENES = []

    GET_SCENES = PythonOperator(
        task_id=GET_SCENES_TASK_NAME,
        python_callable=get_missing_stac_files,
        op_args=["{{ dag_run.conf.limit }}"],
        provide_context=True,
        templates_dict={
            'missing_scenes': "{{ ti.xcom_pull(task_ids={GET_SCENES_TASK_NAME}) }}".format(
                GET_SCENES_TASK_NAME=GET_SCENES_TASK_NAME
            )
        }
    )

    for idx in range(NUN_WORKERS):
        PUBLISH_MISSING_SCENES.append(
            PythonOperator(
                task_id=f"publish_messages_for_missing_scenes_{idx}",
                python_callable=prepare_and_send_messages,
                op_args=[GET_SCENES_TASK_NAME, idx],
                provide_context=True,
            )
        )

    GET_SCENES >> PUBLISH_MISSING_SCENES
