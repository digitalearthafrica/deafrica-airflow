"""
    Script to sync Africa scenes from bulk gzip files or API JSON date request
"""
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from infra.connections import CONN_LANDSAT_SYNC
from infra.sqs_queues import LANDSAT_SYNC_SQS_NAME
from infra.variables import AWS_DEFAULT_REGION
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


def publish_messages(path_list):
    """
    Publish messages
    param message: list of messages
    """

    def post_messages(messages_to_send):
        try:
            sqs_queue = SQS(conn_id=CONN_LANDSAT_SYNC, region=AWS_DEFAULT_REGION)

            sqs_queue.publish_to_sqs_queue(
                queue_name=LANDSAT_SYNC_SQS_NAME,
                messages=messages_to_send,
            )
        except Exception as error:
            logging.error(f"Error sending message to the queue {error}")

    count = 0
    messages = []
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
        post_messages(messages)

    logging.info(f"{count} messages sent successfully :)")
    return count


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


def retrieve_list_of_files(scene_list):
    """
    Function to buils list of assets from USGS S3
    :param scene_list: Scene list from the bulk file
    :return: list of objects [{<display_id>: [<list_assets>]}]
    """
    # Eg. collection02/level-2/standard/etm/2021/196/046/LE07_L2SP_196046_20210101_20210127_02_T1/

    s3 = S3(conn_id=CONN_LANDSAT_SYNC)

    def build_asset_list(scene):
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
        logging.info(f"folder_link {folder_link}")
        response = s3.list_objects(
            bucket_name=USGS_S3_BUCKET_NAME,
            region=USGS_AWS_REGION,
            prefix=folder_link,
            request_payer="requester",
        )

        # if not response.get("Contents"):
        if True:
            logging.error(
                f"Error Listing objects in S3 {USGS_S3_BUCKET_NAME} -"
                f" folder {folder_link} - response {response}"
            )
            notify_email(
                task_name=f'Landsat Identifying - {scene["Date Product Generated L2"]}',
                warning_message=f"Error Listing objects in S3 {USGS_S3_BUCKET_NAME} -"
                f" folder {folder_link} - response {response}",
            )
            return
            # raise Exception(
            #     f"Error Listing objects in S3 {USGS_S3_BUCKET_NAME} folder {folder_link}"
            # )

        mtl_sr_st_files = {}
        for obj in response["Contents"]:
            if "_ST_stac.json" in obj["Key"]:
                mtl_sr_st_files.update({"ST": obj["Key"]})

            elif "_SR_stac.json" in obj["Key"]:
                mtl_sr_st_files.update({"SR": obj["Key"]})

            elif "_MTL.json" in obj["Key"]:
                mtl_sr_st_files.update({"MTL": obj["Key"]})

        if not mtl_sr_st_files:
            raise Exception(
                f'Neither SR nor ST file were found for {scene["Display ID"]}'
            )

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


def identifying_data(file_name: str, date_to_process: str):
    """
    Function to initiate the bulk CSV process
    Warning: Main URL hardcoded, please check for changes in case of the download fails

    :param file_name: (String) File name which will be downloaded
    :param date_to_process: (pendulum.Pendulum) Only process datasets
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
            # TODO remove limitation when in PROD
            path_list = retrieve_list_of_files(scene_list=[s for s in scene_list][0:8])
            # path_list = retrieve_list_of_files(scene_list=scene_list)

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
