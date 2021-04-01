"""
    Script to sync Africa scenes from bulk gzip files or API JSON date request
"""
import csv
import gzip
import json
import logging
import time
from concurrent.futures._base import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

import pandas as pd
import requests
from pendulum import pendulum

from infra.connections import SYNC_LANDSAT_CONNECTION_ID
from infra.variables import SYNC_LANDSAT_CONNECTION_SQS_QUEUE, AWS_DEFAULT_REGION
from utils.aws_utils import SQS
from utils.url_request_utils import request_url, time_process

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
        logging.info(
            f"Sending messages to SQS queue {SYNC_LANDSAT_CONNECTION_SQS_QUEUE}"
        )

        sqs_queue = SQS(conn_id=SYNC_LANDSAT_CONNECTION_ID, region=AWS_DEFAULT_REGION)

        sqs_queue.publish_to_sqs_queue(
            queue_name=SYNC_LANDSAT_CONNECTION_SQS_QUEUE,
            messages=messages_to_send,
        )
        logging.info(f"messages sent {messages_to_send}")

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


def get_allowed_features_json(retrieved_json):
    """
    Function to filter the scenes and allow just the Africa ones
    :param retrieved_json: (dict) retrieved value
    :return: (list)
    """
    logging.info(
        "Filtering the scenes and allowing just the Africa ones based on its PathRow"
    )

    if retrieved_json.get("features") and retrieved_json["features"]:
        # Daily
        return [
            feature
            for feature in retrieved_json["features"]
            if (
                feature.get("properties")
                and feature["properties"].get("landsat:wrs_path")
                and int(
                    f"{feature['properties']['landsat:wrs_path']}{feature['properties']['landsat:wrs_row']}"
                )
                in ALLOWED_PATHROWS
            )
        ]

    elif not retrieved_json.get("features") and retrieved_json.get("properties"):
        # Bulk
        return [retrieved_json]

    return []


def validate_and_send(api_return):
    """
    Validates pathrow and, when valid, send returned value to the queue
    :param api_return: (list) list of values returned from the API
    :return:
    """
    datasets = get_allowed_features_json(retrieved_json=api_return)
    if datasets:
        publish_messages(datasets=datasets)
    else:
        logging.info("No valid scenes were found!")


def request_api_and_send(url: str):
    """
    Function to request the API and send the returned value to the queue.
    If parameters are sent, means we are requesting daily JSON API, or part of its recursion
    If parameters are empty, means we are using bulk CSV files to retrieve the information

    :param url: (String) API URL
    :return: None
    """

    logging.info(f"Requesting URL {url}")

    # Request API
    returned = request_url(url=url)

    logging.info(f"API returned: {returned}")

    # Validate and send to Africa's SQS queue
    validate_and_send(api_return=returned)


def retrieve_json_data_and_send_bulk(display_ids):
    """
    Function to create Python threads which will request the API simultaneously

    :param display_ids: (list) id list from the bulk CSV file
    :return:
    """

    logging.info(f"Starting process")

    # Limit number of threads
    num_of_threads = 16
    logging.info(
        f"Requesting URL {MAIN_URL} adding the display id by the end of the url"
    )

    with ThreadPoolExecutor(max_workers=num_of_threads) as executor:
        tasks = [
            executor.submit(request_api_and_send, f"{MAIN_URL}/{display_id}")
            for display_id in display_ids
        ]
        for future in as_completed(tasks):
            future.result()

    logging.info(f"Whole process finished successfully ;)")


def filter_africa_location(file_path, production_date: pendulum.Pendulum):
    """
    Function to filter just the Africa location based on the WRS Path and WRS Row. All allowed positions are
    informed through the global variable ALLOWED_PATHROWS which is created when this script file is loaded.
    The function also applies filters to skip LANDSAT_4 and Night shots.
    Warning: This function requires high performance from the CPU.

    :param production_date: Filter for L2 Product Generation Date
    :param file_path: (String) Downloaded GZIP file path
    :return: (List) List of Display ids which will be used to retrieve the data from the API.
    """

    logging.info(f"Unzipping and filtering file according to Africa Pathrows")

    # Hack to use when the file is local in /tmp/<file_name>
    # file_path = f"/tmp/{file_name}"
    with gzip.open(file_path, "rt") as csvfile:

        reader = csv.DictReader(csvfile)
        for row in reader:
            # Filter to skip all LANDSAT_4
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
                and (
                    row["Product Generated L2"] == production_date.format("YYYY/MM/DD")
                )
            ):
                yield row["Display ID"]


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

    # TODO use urllib
    url = f"{url}{file_name}"

    # TODO Use pathlib.Path
    file_path = f"/tmp/{file_name}"
    # Todo check if file exists
    # Do a HEAD request and check filesize, only download if different
    with open(file_path, "wb") as f:
        logging.info(f"Downloading file {file_name} to {file_path}")
        downloaded = requests.get(url, stream=True)
        f.write(downloaded.content)

    logging.info(f"{file_name} Downloaded!")
    return file_path


def retrieve_bulk_data(file_name, date_to_process: pendulum.Pendulum):
    """
    Function to initiate the bulk CSV process
    Warning: Main URL hardcoded, please check for changes in case of the download fails

    :param file_name: (String) File name which will be downloaded
    :param pendulum.Pendulum date_to_process: Only process datasets which were generated on this date
    :return: None. Process send information to the queue
    """
    try:
        start_timer = time.time()

        # Download GZIP file
        file_path = download_csv_files(BASE_CSV_URL, file_name)

        # Read file and retrieve the Display ids
        display_id_list = filter_africa_location(
            file_path, production_date=date_to_process
        )
        logging.info(
            f"{len(display_id_list)} found after being filtered on {file_name}"
        )

        if display_id_list:

            # request the API through the display id and send the information to the queue
            retrieve_json_data_and_send_bulk(display_ids=display_id_list)

        else:
            logging.info(
                f"After filtered no valid Ids were found in the file {file_name}"
            )

        logging.info(
            f"File {file_name} processed and sent in {time_process(start=start_timer)}"
        )

    except Exception as error:
        logging.error(error)
        raise error
