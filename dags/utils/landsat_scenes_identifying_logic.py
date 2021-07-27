"""
    Script to sync Africa scenes from bulk gzip files or API JSON date request
"""
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from infra.connections import CONN_LANDSAT_SYNC, CONN_LANDSAT_WRITE
from infra.s3_buckets import LANDSAT_SYNC_BUCKET_NAME
from infra.sqs_queues import LANDSAT_SYNC_SQS_NAME
from infra.variables import REGION
from landsat_scenes_sync.variables import (
    AFRICA_GZ_PATHROWS_URL,
    BASE_BULK_CSV_URL,
    USGS_S3_BUCKET_NAME,
    USGS_AWS_REGION,
)
from utils.aws_utils import SQS, S3
from utils.sync_utils import (
    time_process,
    read_csv_from_gzip,
    download_file_to_tmp,
    read_big_csv_files_from_gzip,
    convert_str_to_date,
    notify_email,
)


def publish_messages(path_list) -> int:
    """
    Publish messages
    param message: list of messages
    """

    def post_messages(messages_to_send):
        try:
            sqs_queue = SQS(conn_id=CONN_LANDSAT_SYNC, region=REGION)

            sqs_queue.publish_to_sqs_queue(
                queue_name=LANDSAT_SYNC_SQS_NAME,
                messages=messages_to_send,
            )
        except Exception as error:
            logging.error(f"Error sending message to the queue {error}")
            return f"Error sending message to the queue {error}"

    count = 0
    messages = []
    sending_error = ""
    flag = False
    for obj in path_list:
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
        sending_error = post_messages(messages)

    if sending_error:
        raise Exception(sending_error)

    logging.info(f"{count} messages sent successfully :)")
    return count


def create_fail_report(folder_path: str, error_message: str):
    """
    Function to save a file to register fails
    :param folder_path:(str)
    :param error_message:(str) Message to add to the log
    :return: None
    """

    file_name = f"fail_{datetime.now()}.txt"
    destination_key = f"fails/{folder_path}{file_name}"

    s3 = S3(conn_id=CONN_LANDSAT_WRITE)
    s3.save_obj_to_s3(
        file=bytes(error_message.encode("UTF-8")),
        destination_key=destination_key,
        destination_bucket=LANDSAT_SYNC_BUCKET_NAME,
    )


def filter_africa_location_from_gzip_file(file_path: Path, production_date: str):
    """
    Function to filter just the Africa location based on the WRS Path and WRS Row.
    All allowed positions are informed through the global variable ALLOWED_PATHROWS which is
    created when this script file is loaded.
    The function also applies filters to skip LANDSAT_4 and Night shots.
    Warning: This function requires high performance from the CPU.

    :param file_path: (Path) Downloaded GZIP file path
    :param production_date: (String) Filter for L2 Product Generation Date
    :return: (List) List of Display ids which will be used to retrieve the data from the API.
    """
    # It'll compare strings so it must have the same format
    production_date = production_date.replace("-", "/")

    # Download updated Pathrows
    africa_pathrows = read_csv_from_gzip(file_path=AFRICA_GZ_PATHROWS_URL)

    # Variable that ensure the log is going to show at the right time
    logging.info("Start Filtering Scenes by Africa location, Just day scenes and date")
    logging.info(f"Unzipping and filtering file according to Africa Pathrows")
    for row in read_big_csv_files_from_gzip(file_path):

        if (
            # Filter to skip all LANDSAT_4
            row.get("Satellite")
            and row["Satellite"] != "LANDSAT_4"
            and row["Satellite"] != "4"
            # Filter to get just day
            and (
                row.get("Day/Night Indicator")
                and row["Day/Night Indicator"].upper() == "DAY"
            )
            # Compare string of dates to ensure that Airflow will process the right date
            and (row["Date Product Generated L2"] == production_date)
            # Filter to get just from Africa
            and (
                row.get("WRS Path")
                and row.get("WRS Row")
                and int(f"{row['WRS Path'].zfill(3)}{row['WRS Row'].zfill(3)}")
                in africa_pathrows
            )
        ):
            yield row


def retrieve_list_of_files(scene_list, update_stac: bool = False):
    """
    Function to buils list of assets from USGS S3
    :param scene_list: Scene list from the bulk file
    :param update_stac: (bool) Flag to force update
    :return: list of objects [{<display_id>: [<list_assets>]}]
    """
    # Eg. collection02/level-2/standard/etm/2021/196/046/LE07_L2SP_196046_20210101_20210127_02_T1/

    s3 = S3(conn_id=CONN_LANDSAT_WRITE)

    def build_asset_list(scene):
        satellite = (
            scene["Satellite"]
            if "LANDSAT" in scene["Satellite"].upper()
            else f'LANDSAT_{scene["Satellite"]}'
        )

        fail_folder_path = (
            f"{satellite}/"
            f'{scene["Date Product Generated L2"]}/'
            f'{scene["Display ID"]}/'
        )

        year_acquired = convert_str_to_date(scene["Date Acquired"]).year
        # USGS changes - for _ when generates the CSV bulk file
        identifier = scene["Sensor Identifier"].lower().replace("_", "-")

        folder_link = (
            "collection02/level-2/standard/{identifier}/{year_acquired}/"
            "{target_path}/{target_row}/{display_id}/".format(
                identifier=identifier,
                year_acquired=year_acquired,
                target_path=scene["WRS Path"].zfill(3),
                target_row=scene["WRS Row"].zfill(3),
                display_id=scene["Display ID"],
            )
        )

        response = s3.list_objects(
            bucket_name=USGS_S3_BUCKET_NAME,
            region=USGS_AWS_REGION,
            prefix=folder_link,
            request_payer="requester",
        )

        if not response.get("Contents"):
            # If there is no Content, generates a file with the issue,
            # then try to send an email and finally returns None
            msg = (
                f"Error Listing objects in S3 {USGS_S3_BUCKET_NAME} - "
                f"folder {folder_link} - response {response}"
            )

            # Create file with the issue
            create_fail_report(folder_path=fail_folder_path, error_message=msg)

            logging.error(msg)

            try:
                notify_email(
                    task_name=f"Landsat Identifying - {scene['Satellite']}",
                    warning_message=msg,
                )
            except Exception as error:
                logging.error(error)

            return

        mtl_sr_st_files = {}
        for obj in response["Contents"]:
            if "_ST_stac.json" in obj["Key"]:
                mtl_sr_st_files.update({"ST": obj["Key"]})

            elif "_SR_stac.json" in obj["Key"]:
                mtl_sr_st_files.update({"SR": obj["Key"]})

            elif "_MTL.json" in obj["Key"]:
                mtl_sr_st_files.update({"MTL": obj["Key"]})

        if not mtl_sr_st_files:
            msg = f'Neither SR nor ST file were found for {scene["Display ID"]}'

            # Create file with the issue
            create_fail_report(folder_path=fail_folder_path, error_message=msg)

            logging.error(msg)

            try:
                notify_email(
                    task_name=f"Landsat Identifying - {scene['Satellite']}",
                    warning_message=msg,
                )
            except Exception as error:
                logging.error(error)

            return

        mtl_sr_st_files.update({'update_stac': update_stac})

        return {scene["Display ID"]: mtl_sr_st_files}

    # Limit number of threads
    num_of_threads = 50
    with ThreadPoolExecutor(max_workers=num_of_threads) as executor:
        logging.info("Retrieving asset list from USGS S3")

        tasks = [
            executor.submit(
                build_asset_list,
                scene,
            )
            for scene in scene_list
        ]

        flag = False
        for future in as_completed(tasks):
            if not flag:
                logging.info("Consulting S3")
                flag = True

            if future:
                yield future.result()


def identifying_data(file_name: str, date_to_process: str, update_stac: bool = False):
    """
    Function to initiate the bulk CSV process
    Warning: Main URL hardcoded, please check for changes in case of the download fails

    :param file_name: (String) File name which will be downloaded
    :param date_to_process: (str) Only process datasets
    :param update_stac: (bool) Bool to force updates
    :return: None. Process send information to the queue
    """
    try:
        start_timer = time.time()

        logging.info(f"Starting Syncing scenes for {date_to_process} changed")

        # Download GZIP file
        file_path = download_file_to_tmp(url=BASE_BULK_CSV_URL, file_name=file_name)

        # Read file and retrieve the Display ids
        scene_list = filter_africa_location_from_gzip_file(
            file_path=file_path, production_date=date_to_process
        )

        if scene_list:
            # request USGS S3 bucket and retrieve list of assets' path
            path_list = retrieve_list_of_files(scene_list=scene_list, update_stac=update_stac)

            # Publish stac to the queue
            messages_sent = publish_messages(path_list=path_list)
            logging.info(f"Messages sent {messages_sent}")

        else:
            logging.info(
                f"After filtered no valid or new Ids were found in the file {file_name}"
            )

        logging.info(
            f"File {file_name} processed and sent in {time_process(start=start_timer)}"
        )

        logging.info(f"Whole process finished successfully ;)")

    except Exception as error:
        logging.error(error)
        raise error
