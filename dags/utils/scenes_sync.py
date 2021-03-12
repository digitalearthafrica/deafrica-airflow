"""
    Script to sync Africa scenes from bulk gzip files or API JSON date request
"""
import gzip
import json
import logging
import threading
import requests
import pandas as pd

from infra.connections import SYNC_LANDSAT_CONNECTION_ID
from infra.variables import SYNC_LANDSAT_CONNECTION_SQS_QUEUE

from utils.url_request_utils import request_url, publish_to_sqs_queue

ALLOWED_PATHROWS = set(pd.read_csv(
        "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/deafrica-usgs-pathrows.csv.gz",
        header=None,
    ).values.ravel()
)


def publish_messages(datasets):
    """
    Publish messages
    param message: list of messages
    """

    def post_messages(messages_to_send):
        logging.info(f'Sending messages to SQS queue {SYNC_LANDSAT_CONNECTION_SQS_QUEUE}')
        publish_to_sqs_queue(
            aws_conn_id=SYNC_LANDSAT_CONNECTION_ID,
            queue_name=SYNC_LANDSAT_CONNECTION_SQS_QUEUE,
            messages=messages_to_send
        )
        logging.info(f'messages sent {messages_to_send}')

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
    logging.info("Filtering the scenes and allowing just the Africa ones based on its PathRow")

    if retrieved_json.get("features") and retrieved_json["features"]:
        return [
            feature
            for feature in retrieved_json["features"]
            if (
                feature.get("properties")
                and feature["properties"].get("landsat:wrs_path")
                and int(
                    f"{feature['properties']['landsat:wrs_path']}{feature['properties']['landsat:wrs_row']}"
                ) in ALLOWED_PATHROWS
            )
        ]

    elif not retrieved_json.get("features") and retrieved_json.get("properties"):
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


def request_api_and_send(url: str, params=None):
    """
    Function to request the API and send the returned value to the queue.
    If parameters are sent, means we are requesting daily JSON API, or part of its recursion
    If parameters are empty, means we are using bulk CSV files to retrieve the information

    :param url: (String) API URL
    :param params: (Dict) Parameters to add to the URL
    :return: None
    """
    if params is None:
        params = {}

    logging.info(f"Requesting URL {url} with parameters {params}")

    # Request API
    returned = request_url(url=url, params=params)

    logging.info(f"API returned: {returned}")

    logging.debug(f"Found {returned['meta']['found']}")

    # Validate and send to Africa's SQS queue
    validate_and_send(api_return=returned)

    # Retrieve daily requests
    if params:

        logging.info('Checking for additional page data')
        found = False
        if (
            returned.get("meta")
            and returned["meta"].get("page")
            and returned["meta"].get("limit")
            and returned["meta"].get("found")
            and returned["meta"].get("returned")
        ):
            if (
                returned["meta"]["returned"] == returned["meta"]["limit"]
                and (returned["meta"]["page"] * returned["meta"]["limit"]) < returned["meta"]["found"]
            ):
                found = True
                page = returned["meta"]["page"] + 1

                logging.info(f"Additional data found, requesting page {page}")

                params.update({"page": page})
                request_api_and_send(url=url, params=params)

                logging.info(
                    "*-*-*-*-*-**-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*"
                )
        if not found:
            logging.info('No additional page data was found')

    logging.info("Process Finished !! :)")


def retrieve_json_data_and_send(date=None, display_ids=None):
    """
    Function to create Python threads which will request the API simultaneously
    If start_date and end_date are sent, means we are requesting daily JSON API
    If display_ids is sent, means we are using bulk CSV files to retrieve the information

    :param date: (datetime) Date to request the API
    :param display_ids: (list) id list from the bulk CSV file
    :return:
    """
    # Example of API URLs
    # https://landsatlook.usgs.gov/sat-api/stac/search?collection=landsat-c2l2-sr&time=2020-12-07 # outdated
    # https://landsatlook.usgs.gov/sat-api/collections/landsat-c2l2-sr/items?time=2020-12-07&page=2
    # https://landsatlook.usgs.gov/sat-api/collections/landsat-c2l2-sr/items/LC08_L1GT_166112_20210123_20210123_02_RT
    # https://landsatlook.usgs.gov/sat-api/collections/landsat-c2l2-sr/items?time=2018-02-01T00:00:00Z/2018-02-01T23:59:59Z&bbox=%5B-26.359944882003788,%20-47.96476498374171,%2064.4936701740102,%2038.34459242512347%5D&limit=500

    try:

        if not date and not display_ids:
            msg = "Date is required for daily JSON request. For a bulk CSV request, Display_ids is required"
            logging.error(msg)
            raise Exception(msg)

        main_url = "https://landsatlook.usgs.gov/sat-api/collections/landsat-c2l2-sr/items"
        africa_bbox = [
            -26.359944882003788,
            -47.96476498374171,
            64.4936701740102,
            38.34459242512347,
        ]

        if not display_ids:

            logging.info(f"Starting Daily process")

            params = {
                "limit": 100,
                "bbox": json.dumps(africa_bbox),
                "time": date.date().isoformat(),
            }

            # Request daily JSON API
            request_api_and_send(url=main_url, params=params)

        else:

            logging.info(f"Starting Bulk process")

            # Limit number of threads
            num_of_threads = 16
            count_tasks = len(display_ids)
            logging.info(
                f"Simultaneously {num_of_threads} process/requests (Python threads) to send {count_tasks} messages"
            )
            logging.info(
                f"Requesting URL {main_url} adding the display id by the end of the url"
            )

            while count_tasks > 0:

                if count_tasks < num_of_threads:
                    num_of_threads = count_tasks

                # Create threads for the ids from the bulk CSV file
                thread_list = [
                    threading.Thread(
                        target=request_api_and_send,
                        args=(f"{main_url}/{display_id}",),
                    )
                    for display_id in display_ids[
                        (count_tasks - num_of_threads): count_tasks
                    ]
                ]

                # Start Threads
                [start_thread.start() for start_thread in thread_list]

                logging.info(
                    f"Running threads to retrieve and send "
                    f"ids {display_ids[(count_tasks - num_of_threads):count_tasks]}"
                )
                # Wait all {num_of_threads} threads finish to start {num_of_threads} more
                [join_thread.join() for join_thread in thread_list]

                count_tasks -= num_of_threads

    except Exception as error:
        logging.error(error)
        raise error


def read_csv(file_path):
    """
    Function to read inside of the GZIP file and return row by row as a
    dict composed by the header as key and the line as value

    :param file_path: (String) Downloaded GZIP file path
    :return: (dict) Row of the file
    """
    header = []

    def build_dict(built_header, bvalue):
        """
        Function to through the CSV row build a dict. in case of the header being None, it will assume
        that the informed row is the header and will return the header as a list
        :param built_header: (dict) Header of the CSV
        :param bvalue: (bstr) Row of the CSV
        :return: (list/dict) list if the header isn't informed and a dict with the header values as key
        values and the row as values if the header is informed
        """
        values = str(bvalue).rstrip("\n").split(",")
        if not built_header:
            return values
        else:
            return {header[index]: values[index] for index in range(0, len(header))}

    for row in gzip.open(file_path, "rt"):
        # Gzip library does not skip header line, instead reads line by line returning a byte string,
        # so the inner-function will build the right dict based on the header
        if not header:
            header = build_dict(header, row)
        else:
            yield build_dict(header, row)


def filter_africa_location(file_path):
    """
    Function to filter just the Africa location based on the WRS Path and WRS Row. All allowed positions are
    informed through the global variable ALLOWED_PATHROWS which is created when this script file is loaded.
    The function also applies filters to skip LANDSAT_4 and Night shots.
    Warning: This function requires high performance from the CPU.

    :param file_path: (String) Downloaded GZIP file path
    :return: (List) List of Display ids which will be used to retrieve the data from the API.
    """

    logging.info(f"Unzipping and filtering file according to Africa Pathrows")
    return [
        row["Display ID"]
        for row in read_csv(file_path)
        # Filter to skip all LANDSAT_4
        if (
                   row.get("Satellite")
                   and row["Satellite"] != "LANDSAT_4"
           )
        # Filter to get just from Africa
        and (
            row.get("WRS Path")
            and row.get("WRS Row")
            and int(f"{row['WRS Path']}{row['WRS Row']}") in ALLOWED_PATHROWS
        )
        # Filter to get just day
        and (
                   row.get('Day/Night Indicator')
                   and row['Day/Night Indicator'].upper() == 'DAY'
           )
    ]


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

    url = f"{url}{file_name}"

    file_path = f"/tmp/{file_name}"
    with open(file_path, "wb") as f:
        logging.info(f"Downloading file {file_name} to {file_path}")
        downloaded = requests.get(url, stream=True)
        f.write(downloaded.content)

    logging.info(f"{file_name} Downloaded!")
    return file_path


def retrieve_bulk_data(file_name):
    """
    Function to initiate the bulk CSV process
    Warning: Main URL hardcoded, please check for changes in case of the download fails

    :param file_name: (String) File name which will be downloaded
    :return: None. Process send information to the queue
    """
    try:
        # Main URL
        main_url = (
            "https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/"
        )

        # Download GZIP file
        file_path = download_csv_files(main_url, file_name)

        # Hack to use when the file is local in /tmp/<file_name>
        # file_path = f"/tmp/{file_name}"

        # Read file and retrieve the Display ids
        display_id_list = filter_africa_location(file_path)
        logging.info(f"{len(display_id_list)} found after being filtered")

        if display_id_list:

            # request the API through the display id and send the information to the queue
            retrieve_json_data_and_send(display_ids=display_id_list)
        else:
            logging.info(f"After filtered no valid Ids were found in the file {file_name}")

    except Exception as error:
        logging.error(error)
        raise error
