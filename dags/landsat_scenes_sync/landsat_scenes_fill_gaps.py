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
import gzip
import json
import logging
import traceback
from datetime import datetime
from typing import Optional

from airflow import DAG
from airflow.contrib.hooks.aws_sqs_hook import SQSHook
from airflow.operators.python_operator import PythonOperator
from odc.aws.queue import publish_messages

from infra.connections import CONN_LANDSAT_SYNC
from infra.s3_buckets import LANDSAT_SYNC_BUCKET_NAME
from infra.sqs_queues import LANDSAT_SYNC_USGS_SNS_FILTER_SQS_NAME
from infra.variables import REGION
from landsat_scenes_sync.variables import STATUS_REPORT_FOLDER_NAME
from utils.aws_utils import S3

REPORTING_PREFIX = "status-report/"
# This process is manually run
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


def post_messages(message_list) -> None:
    """
    Publish messages
    :param message_list:(list) list of messages
    :return:(None)
    """

    count = 0
    messages = []
    sqs_conn = SQSHook(aws_conn_id=CONN_LANDSAT_SYNC)
    sqs_hook = sqs_conn.get_resource_type(
        resource_type="sqs", region_name=REGION
    )
    queue = sqs_hook.get_queue_by_name(QueueName=LANDSAT_SYNC_USGS_SNS_FILTER_SQS_NAME)

    logging.info("Sending messages")
    for message_dict in message_list:
        message = {
            "Id": str(count),
            "MessageBody": str(json.dumps(message_dict)),
        }

        messages.append(message)
        count += 1

        # Send 10 messages per time
        if count % 10 == 0:
            publish_messages(queue, messages)
            messages = []

    # Post the last messages if there are any
    if len(messages) > 0:
        publish_messages(queue, messages)

    logging.info(f"{count} messages sent successfully")


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


def build_message(missing_scene_paths, update_stac):
    """

    """
    message_list = []
    for path in missing_scene_paths:
        landsat_product_id = str(path.strip("/").split("/")[-1])
        if not landsat_product_id:
            raise Exception(f'It was not possible to build product ID from path {path}')
        message_list.append(
            {
                "Message": {
                    "landsat_product_id": landsat_product_id,
                    "s3_location": str(path),
                    "update_stac": update_stac
                }
            }
        )
    return message_list


def fill_the_gap(landsat: str, scenes_limit: Optional[int] = None) -> None:
    """
    Function to retrieve the latest gap report and create messages to the filter queue process.

    :param landsat:(str) satellite name
    :param scenes_limit:(str) limit of how many scenes will be filled
    :return:(None)
    """
    try:
        logging.info("Looking for latest report")
        latest_report = find_latest_report(landsat=landsat)
        logging.info(f"Latest report found {latest_report}")

        if not latest_report:
            logging.error("Report not found")
            raise RuntimeError("Report not found!")
        else:
            logging.info("Reading missing scenes from the report")

            s3 = S3(conn_id=CONN_LANDSAT_SYNC)

            missing_scene_file_gzip = s3.get_s3_contents_and_attributes(
                bucket_name=LANDSAT_SYNC_BUCKET_NAME,
                region=REGION,
                key=latest_report,
            )

            # This should just use Pandas. It's already a dependency.
            missing_scene_paths = [
                scene_path
                for scene_path in gzip.decompress(missing_scene_file_gzip).decode("utf-8").split("\n")
                if scene_path
            ]

            logging.info(f"Number of scenes found {len(missing_scene_paths)}")
            logging.info(f"Example scenes: {missing_scene_paths[0:10]}")

            logging.info(f"Limited: {'No limit' if scenes_limit else scenes_limit}")
            if scenes_limit:
                missing_scene_paths = missing_scene_paths[:int(scenes_limit)]

            update_stac = False
            if 'update' in latest_report:
                logging.info('FORCED UPDATE FLAGGED!')
                update_stac = True

            messages_to_send = build_message(
                missing_scene_paths=missing_scene_paths,
                update_stac=update_stac
            )

            logging.info("Publishing messages")
            post_messages(message_list=messages_to_send)
    except Exception as error:
        logging.error(error)
        # print traceback but does not stop execution
        traceback.print_exc()
        raise error


with DAG(
        "landsat_scenes_fill_the_gap",
        default_args=default_args,
        schedule_interval=SCHEDULE_INTERVAL,
        tags=["Landsat_scenes", "fill the gap"],
        catchup=False,
) as dag:

    PROCESSES = []
    satellites = [
        "landsat_8",
        "landsat_7",
        "landsat_5"
    ]

    for sat in satellites:
        PROCESSES.append(
            PythonOperator(
                task_id=f"{sat}_fill_the_gap",
                python_callable=fill_the_gap,
                op_kwargs=dict(landsat=sat, scenes_limit="{{ dag_run.conf.scenes_limit }}"),
            )
        )


    PROCESSES
