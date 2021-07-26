"""
# Read report and generate messages to fill missing scenes

#### Utility utilization
The DAG can be parameterized with run time configurations `scenes_limit`, which receives a INT as value.

* The option scenes_limit limit the number of scenes to be read from the report,
therefore limit the number of messages to be sent

#### example conf in json format
    {
        "scenes_limit":10
    }

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
    REGION,
)
from landsat_scenes_sync.variables import (
    STATUS_REPORT_FOLDER_NAME,
)
from utils.aws_utils import S3, SQS

REPORTING_PREFIX = "status-report/"
# Dev does not need to be updated
SCHEDULE_INTERVAL = None

default_args = {
    "owner": "RODRIGO",
    "start_date": datetime(2021, 6, 7),
    "email": ["systems@digitalearthafrica.org"],
    "email_on_failure": True,
    "email_on_success": True,
    "email_on_retry": False,
    "retries": 0,
    "version": "0.0.1",
}


def publish_messages(message_list) -> None:
    """
    Publish messages
    :param message_list:(list) list of messages
    :return:(None)
    """

    def post_messages(messages_to_send):
        try:
            sqs_queue = SQS(conn_id=CONN_LANDSAT_SYNC, region=REGION)

            sqs_queue.publish_to_sqs_queue(
                queue_name=LANDSAT_SYNC_USGS_SNS_FILTER_SQS_NAME,
                messages=messages_to_send,
            )
        except Exception as error:
            logging.error(f"Error sending message to the queue {error}")
            return True

    count = 0
    messages = []
    error_flag = False
    logging.info("sending messages")
    for obj in message_list:
        message = {
            "Id": str(count),
            "MessageBody": str(json.dumps(obj)),
        }

        messages.append(message)

        count += 1
        # Send 10 messages per time
        if count % 10 == 0:
            error_flag = post_messages(messages)
            messages = []

    # Post the last messages if there are any
    if len(messages) > 0:
        error_flag = post_messages(messages)

    if error_flag:
        raise Exception("There was an error sending messages")

    logging.info(f"{count} messages sent successfully :)")


def find_latest_report(landsat: str) -> str:
    """
    Function to find the latest gap report
    :param landsat:(str)satellite name
    :return:(str) return the latest report file name
    """
    continuation_token = None
    list_reports = []
    while True:
        s3 = S3(conn_id=CONN_LANDSAT_SYNC)

        resp = s3.list_objects(
            bucket_name=LANDSAT_SYNC_BUCKET_NAME,
            region=REGION,
            prefix=f"{STATUS_REPORT_FOLDER_NAME}/",
            continuation_token=continuation_token,
        )

        if not resp.get("Contents"):
            raise Exception(
                f"Report not found at "
                f"{LANDSAT_SYNC_BUCKET_NAME}/{STATUS_REPORT_FOLDER_NAME}/"
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
        if resp.get("NextContinuationToken"):
            continuation_token = resp["NextContinuationToken"]
        else:
            break

    list_reports.sort()
    return list_reports[-1] if list_reports else ""


def fill_the_gap(landsat: str, scenes_limit: int) -> None:
    """
    Function to retrieve the latest gap report and create messages to the filter queue process.
    :param landsat:(str) satellite name
    :return:(None)
    """

    logging.info("Looking for latest report")
    latest_report = find_latest_report(landsat=landsat)
    logging.info(f"Latest report found {latest_report}")

    if not latest_report:
        logging.error("Report not found")
    else:
        logging.info("Reading missing scenes from the report")

        s3 = S3(conn_id=CONN_LANDSAT_SYNC)

        missing_scene_file = s3.get_s3_contents_and_attributes(
            bucket_name=LANDSAT_SYNC_BUCKET_NAME,
            region=REGION,
            key=latest_report,
        )

        if scenes_limit:
            missing_scene_paths = [
                scene_path
                for scene_path in missing_scene_file.decode("utf-8").split("\n")
                if scene_path
            ][0: int(scenes_limit)]
        else:
            missing_scene_paths = [
                scene_path
                for scene_path in missing_scene_file.decode("utf-8").split("\n")
                if scene_path
            ]

        update_stac = False
        if 'update_stac' in missing_scene_paths:
            logging.info('Forced stac update flagged!')
            update_stac = True
            missing_scene_paths.remove('update_stac')

        logging.info(f"missing_scene_paths {missing_scene_paths[0:10]}")

        logging.info(f"Number of scenes found {len(missing_scene_paths)}")

        logging.info("Publishing messages")
        publish_messages(
            message_list=[
                {
                    "Message": {
                        "landsat_product_id": str(path[0:-1].split("/")[-1]),
                        "s3_location": str(path),
                        "update_stac": update_stac
                    }
                }
                for path in missing_scene_paths
            ]
        )


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
                op_kwargs=dict(landsat=sat, update_stac="{{ dag_run.conf.scenes_limit }}"),
            )
        )

    END = DummyOperator(task_id="end-tasks")

    START >> processes >> END
