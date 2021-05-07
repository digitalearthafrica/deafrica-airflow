"""
    Script to sync Africa scenes from bulk gzip files or API JSON date request
"""
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from infra.connections import SYNC_LANDSAT_CONNECTION_ID
from infra.sqs_queues import LANDSAT_SYNC_SQS_QUEUE
from infra.variables import AWS_DEFAULT_REGION
from landsat_scenes_sync.variables import (
    AFRICA_GZ_PATHROWS_URL,
    BASE_BULK_CSV_URL,
    USGS_API_INDIVIDUAL_ITEM_URL,
)
from utils.aws_utils import SQS
from utils.sync_utils import (
    request_url,
    time_process,
    read_csv_from_gzip,
    download_file_to_tmp,
    read_big_csv_files_from_gzip,
)


def publish_messages(datasets):
    """
    Publish messages
    param message: list of messages
    """

    def post_messages(messages_to_send):
        try:
            sqs_queue = SQS(
                conn_id=SYNC_LANDSAT_CONNECTION_ID, region=AWS_DEFAULT_REGION
            )

            sqs_queue.publish_to_sqs_queue(
                queue_name=LANDSAT_SYNC_SQS_QUEUE,
                messages=messages_to_send,
            )
        except Exception as error:
            logging.error(f"Error sending message to the queue {error}")

    count = 0
    messages = []
    for dataset in datasets:
        message = {
            "Id": str(count),
            "MessageBody": json.dumps(dataset),
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


def request_usgs_api(url: str):
    """
    Function to handle exceptions from request_url
    :param url: (str) url to be requested
    :return: API return from request_url function
    """ ""
    try:
        response = request_url(url=url)
        if response.get("stac_version"):
            if response["stac_version"] == "1.0.0-beta.2":
                return response
            else:
                logging.info(f"stac_version {response.get('stac_version')}")

    except Exception as error:
        # If the request return an error, just log and keep going
        logging.error(f"Error requesting API: {error}")
    return None


def retrieve_stac_from_api(display_ids):
    """
    Function to create Python threads which will request the API simultaneously

    :param display_ids: (list) id list from the bulk CSV file
    :return:
    """

    # Limit number of threads
    num_of_threads = 50
    with ThreadPoolExecutor(max_workers=num_of_threads) as executor:
        log = False
        tasks = []
        for display_id in display_ids:
            if not log:
                logging.info(f"Requesting USGS API")
                logging.info(
                    f"Requesting URL {USGS_API_INDIVIDUAL_ITEM_URL} adding the display id by the end of the url"
                )
            tasks.append(
                executor.submit(
                    request_usgs_api, f"{USGS_API_INDIVIDUAL_ITEM_URL}/{display_id}"
                )
            )

        for future in as_completed(tasks):
            if future.result():
                yield future.result()


def filter_africa_location_from_gzip_file(file_path: Path, production_date: str):
    """
    Function to filter just the Africa location based on the WRS Path and WRS Row. All allowed positions are
    informed through the global variable ALLOWED_PATHROWS which is created when this script file is loaded.
    The function also applies filters to skip LANDSAT_4 and Night shots.
    Warning: This function requires high performance from the CPU.

    :param file_path: (Path) Downloaded GZIP file path
    :param production_date: (String) Filter for L2 Product Generation Date ("YYYY/MM/DD" or "YYYY-MM-DD")
    :return: (List) List of Display ids which will be used to retrieve the data from the API.
    """
    # It'll compare strings so it must have the same format
    production_date = production_date.replace("-", "/")

    # Download updated Pathrows
    africa_pathrows = read_csv_from_gzip(file_path=AFRICA_GZ_PATHROWS_URL)

    # Variable that ensure the log is going to show at the right time
    log = False
    for row in read_big_csv_files_from_gzip(file_path):
        if not log:
            logging.info(
                "Start Filtering Scenes by Africa location, Just day scenes and date"
            )
            logging.info(f"Unzipping and filtering file according to Africa Pathrows")
            log = True

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
                and int(f"{row['WRS Path']}{row['WRS Row']}") in africa_pathrows
            )
        ):
            yield row["Display ID"]


def sync_data(file_name: str, date_to_process: str):
    """
    Function to initiate the bulk CSV process
    Warning: Main URL hardcoded, please check for changes in case of the download fails

    :param file_name: (String) File name which will be downloaded
    :param date_to_process: (pendulum.Pendulum) Only process datasets
    :return: None. Process send information to the queue
    """
    try:
        start_timer = time.time()

        logging.info(f"Starting Syncing scenes for {date_to_process}")

        # Download GZIP file
        file_path = download_file_to_tmp(url=BASE_BULK_CSV_URL, file_name=file_name)

        # Read file and retrieve the Display ids
        display_id_list = filter_africa_location_from_gzip_file(
            file_path=file_path, production_date=date_to_process
        )

        if display_id_list:
            # request the API through the display id and send the information to the queue
            stac_list = retrieve_stac_from_api(display_ids=display_id_list)

            # Publish stac to the queue
            messages_sent = publish_messages(datasets=stac_list)
            logging.info(f"Messages sent {messages_sent}")
            # logging.info(f"Messages sent {len([s for s in stac_list])}")

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
