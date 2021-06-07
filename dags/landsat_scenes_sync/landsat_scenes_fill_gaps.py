"""
# Read report and generate messages to fill missing scenes
"""
import json
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from infra.connections import CONN_LANDSAT_WRITE, CONN_LANDSAT_SYNC
from infra.s3_buckets import LANDSAT_SYNC_BUCKET_NAME
from infra.sqs_queues import LANDSAT_SYNC_USGS_SNS_FILTER_SQS_NAME
from infra.variables import (
    AWS_DEFAULT_REGION,
    LANDSAT_SYNC_S3_STATUS_REPORT_FOLDER_NAME,
)
from landsat_scenes_sync.variables import USGS_S3_BUCKET_PATH
from utils.aws_utils import S3, SQS

REPORTING_PREFIX = "status-report/"
SCHEDULE_INTERVAL = "@weekly"

default_args = {
    "owner": "rodrigo.carvalho",
    "start_date": datetime(2021, 6, 7),
    "email": ["rodrigo.carvalho@ga.gov.au", "alex.leith@ga.gov.au"],
    "email_on_failure": True,
    "email_on_success": True,
    "email_on_retry": False,
    "retries": 0,
    "version": "0.0.1",
}


def publish_messages(message_list) -> None:
    """
    Publish messages
    param message: list of messages
    """

    def post_messages(messages_to_send):
        try:
            sqs_queue = SQS(conn_id=CONN_LANDSAT_SYNC, region=AWS_DEFAULT_REGION)

            sqs_queue.publish_to_sqs_queue(
                queue_name=LANDSAT_SYNC_USGS_SNS_FILTER_SQS_NAME,
                messages=messages_to_send,
            )
        except Exception as error:
            logging.error(f"Error sending message to the queue {error}")

    count = 0
    messages = []
    flag = False
    for obj in message_list:
        if not flag:
            logging.info("sending messages")
            flag = True
        message = {
            "Id": str(count),
            "MessageBody": json.dumps(obj),
        }

        messages.append(message)

        count += 1
        # Send 10 messages per time
        if count % 10 == 0:
            post_messages(messages)
            messages = []

    # Post the last messages if there are any
    if len(messages) > 0:
        post_messages(messages)

    logging.info(f"{count} messages sent successfully :)")


def find_latest_report(landsat: str) -> str:
    """
    Function to find the latest gap report
    :param landsat:
    :return:
    """
    continuation_token = None
    list_reports = []
    while True:
        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.

        s3 = S3(conn_id=CONN_LANDSAT_WRITE)

        resp = s3.list_objects(
            bucket_name=LANDSAT_SYNC_BUCKET_NAME,
            region=AWS_DEFAULT_REGION,
            prefix=f"{LANDSAT_SYNC_S3_STATUS_REPORT_FOLDER_NAME}/",
            continuation_token=continuation_token,
        )

        if not resp.get("Contents"):
            raise Exception(
                f"Report not found at "
                f"{LANDSAT_SYNC_BUCKET_NAME}/{LANDSAT_SYNC_S3_STATUS_REPORT_FOLDER_NAME}/"
                f"  - returned {resp}"
            )

        list_reports.extend(
            [
                obj["Key"]
                for obj in resp["Contents"]
                if landsat in obj["Key"] and "orphaned" not in obj["Key"]
            ]
        )

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        if resp.get("NextContinuationToken"):
            continuation_token = resp["NextContinuationToken"]
        else:
            break

    list_reports.sort()
    return list_reports[-1] if list_reports else ""


def retrieve_status_report(landsat: str):
    """

    :param landsat:
    :return:
    """

    logging.info("Looking for latest report")
    latest_report = find_latest_report(landsat=landsat)
    logging.info(f"Latest report found {latest_report}")

    if not latest_report:
        logging.error("Report not found")
    else:
        logging.info("Reading missing scenes from the report")

        s3 = S3(conn_id=CONN_LANDSAT_WRITE)

        missing_scene_file = s3.get_s3_contents_and_attributes(
            bucket_name=LANDSAT_SYNC_BUCKET_NAME,
            region=AWS_DEFAULT_REGION,
            key=latest_report,
        )

        missing_scene_paths = [
            scene_path
            for scene_path in missing_scene_file.decode("utf-8").split("\n")
            if scene_path
        ]
        logging.info(f"missing_scene_paths {missing_scene_paths[0:10]}")

        logging.info(f"Number of scenes found {len(missing_scene_paths)}")

        logging.info("Publishing messages")
        publish_messages(
            message_list=[
                {
                    "Message": {
                        "landsat_product_id": path[0:-1].split("/")[-1],
                        "s3_location": path,
                    }
                }
                for path in missing_scene_paths
            ]
        )


def fill_the_gap(landsat: str) -> None:
    """
        Function responsible to read status report and send them as messages to the queue
    :param landsat:(str) Satellite name
    :return: (None)
    """

    retrieve_status_report(landsat)


with DAG(
    "landsat_scenes_fill_the_gap",
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
    tags=["Landsat_scenes", "fill the gap"],
    catchup=False,
) as dag:
    START = DummyOperator(task_id="start-tasks")

    processes = []
    satellites = ["landsat_8", "landsat_7", "Landsat_5"]

    for sat in satellites:
        processes.append(
            PythonOperator(
                task_id=f"{sat}_fill_the_gap",
                python_callable=fill_the_gap,
                op_kwargs=dict(landsat=sat),
            )
        )

    END = DummyOperator(task_id="end-tasks")

    START >> processes >> END
