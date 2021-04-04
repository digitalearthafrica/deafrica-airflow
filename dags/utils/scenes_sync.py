"""
    Script to sync Africa scenes from bulk gzip files or API JSON date request
"""
import csv
import gzip
import json
import logging
import time
import requests

from concurrent.futures._base import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from airflow.models import Variable

from infra.connections import SYNC_LANDSAT_CONNECTION_ID
from infra.variables import SYNC_LANDSAT_CONNECTION_SQS_QUEUE, AWS_DEFAULT_REGION
from utils.aws_utils import SQS
from utils.sync_utils import request_url, time_process, convert_str_to_date

ALLOWED_PATHROWS = set(
    pd.read_csv(
        "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/deafrica-usgs-pathrows.csv.gz",
        header=None,
    ).values.ravel()
)

BASE_CSV_URL = "https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/"

MAIN_URL = "https://landsatlook.usgs.gov/sat-api/collections/landsat-c2l2-sr/items"


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
                queue_name=SYNC_LANDSAT_CONNECTION_SQS_QUEUE,
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


def request_api(url: str):
    """
    Function to request the API and send the returned value to the queue.
    If parameters are sent, means we are requesting daily JSON API, or part of its recursion
    If parameters are empty, means we are using bulk CSV files to retrieve the information

    :param url: (String) API URL
    :return: None
    """
    try:

        # return 'Requested API'
        # Request API
        returned = request_url(url=url)

        return returned

    except Exception as error:
        logging.error(f"Error Requesting {url} ERROR: {error}")


def retrieve_stac_from_api(display_ids):
    """
    Function to create Python threads which will request the API simultaneously

    :param display_ids: (list) id list from the bulk CSV file
    :return:
    """

    logging.info(f"Starting process")

    # Limit number of threads
    num_of_threads = 50
    logging.info(
        f"Requesting URL {MAIN_URL} adding the display id by the end of the url"
    )

    with ThreadPoolExecutor(max_workers=num_of_threads) as executor:
        tasks = [
            executor.submit(request_api, f"{MAIN_URL}/{display_id}")
            for display_id in display_ids
        ]

        for future in as_completed(tasks):
            if future.result():
                yield future.result()


def filter_africa_location(file_path: Path):
    """
    Function to filter just the Africa location based on the WRS Path and WRS Row. All allowed positions are
    informed through the global variable ALLOWED_PATHROWS which is created when this script file is loaded.
    The function also applies filters to skip LANDSAT_4 and Night shots.
    Warning: This function requires high performance from the CPU.

    :param file_path: (Path) Downloaded GZIP file path
    :return: (List) List of Display ids which will be used to retrieve the data from the API.
    """

    # Get value of last date from Airflow
    saved_last_date = (
        convert_str_to_date(Variable.get("last_date"))
        if Variable.get("last_date", default_var=False)
        else ""
    )

    logging.info(
        f"Unzipping and filtering file according to Africa Pathrows, "
        f"day scenes and date {saved_last_date if saved_last_date else ''}"
    )

    with gzip.open(file_path, "rt") as csv_file:
        # This variable will update airflow variable,
        # in case of by the end of this process the system finds a higher date
        last_date = None

        for row in csv.DictReader(csv_file):

            generated_date = convert_str_to_date(row["Date Product Generated L2"])

            if not last_date or generated_date > last_date:
                last_date = generated_date
                logging.info(f"Add value to last_date {last_date}")

            if (
                row.get("Satellite")
                and row["Satellite"] != "LANDSAT_4"
                # Filter to get just from Africa
                and (
                    row.get("WRS Path")
                    and row.get("WRS Row")
                    and int(f"{row['WRS Path']}{row['WRS Row']}") in ALLOWED_PATHROWS
                )
                # Filter to get just day
                and (
                    row.get("Day/Night Indicator")
                    and row["Day/Night Indicator"].upper() == "DAY"
                )
                # Filter by the generated date comparing to the last Airflow interaction
                and (not saved_last_date or saved_last_date < generated_date)
            ):
                yield row["Display ID"]

        if not saved_last_date or saved_last_date < last_date:
            logging.info(f"Updating Airflow variable to {last_date}")
            Variable.set("last_date", last_date)


def download_csv_files(url, file_name):
    """
    Function to download bulk CSV file from the informed server.
    The file will be saved in the local machine under the /tmp/ folder, so the OS will delete that accordingly
    with its pre-defined configurations.
    Warning: The server shall have at least 3GB of free storage.

    :param url:(String) URL path for the API
    :param file_name: (String) File name which will be downloaded
    :return: (String) File path where it was downloaded. Hardcoded for /tmp/
    """

    url = urlparse(f"{url}{file_name}")
    file_path = Path(f"/tmp/{file_name}")

    # check if file exists and comparing size against cloud file
    if file_path.exists():
        file_size = file_path.stat().st_size
        head = requests.head(url.geturl())

        if hasattr(head, "headers") and head.headers.get("Content-Length"):
            server_file_size = head.headers["Content-Length"]
            logging.info(
                f"Comparing sizes between local saved file and server hosted file,"
                f" local file size : {type(file_size)} server file size: {type(server_file_size)}"
            )

            if int(file_size) == int(server_file_size):
                logging.info("Already updated!!")
                return ""

    logging.info(f"Downloading file {file_name} to {file_path}")
    downloaded = requests.get(url.geturl(), stream=True)
    file_path.write_bytes(downloaded.content)

    logging.info(f"{file_name} Downloaded!")
    return file_path


def sync_data(file_name):
    """
    Function to initiate the bulk CSV process
    Warning: Main URL hardcoded, please check for changes in case of the download fails

    :param file_name: (String) File name which will be downloaded
    :return: None. Process send information to the queue
    """
    try:
        start_timer = time.time()

        logging.info("Starting Syncing scenes")

        # Download GZIP file
        logging.info("Start downloading files")
        file_path = download_csv_files(BASE_CSV_URL, file_name)

        if file_path:

            # Read file and retrieve the Display ids
            logging.info(
                "Start Filtering Scenes by Africa location, Just day scenes and date Test"
            )
            display_id_list = filter_africa_location(file_path=file_path)

            if display_id_list:
                # request the API through the display id and send the information to the queue
                logging.info(
                    "Start process of consulting API and sending stac to the queue"
                )
                stac_list = retrieve_stac_from_api(display_ids=display_id_list)

                # Publish stac to the queue
                logging.info(
                    f"Sending messages to SQS queue {SYNC_LANDSAT_CONNECTION_SQS_QUEUE}"
                )
                messages_sent = publish_messages(datasets=stac_list)
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
