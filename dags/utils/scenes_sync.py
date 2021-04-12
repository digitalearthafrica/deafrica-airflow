"""
    Script to sync Africa scenes from bulk gzip files or API JSON date request
"""
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from infra.connections import SYNC_LANDSAT_CONNECTION_ID
from infra.sqs_queues import SYNC_LANDSAT_CONNECTION_SQS_QUEUE
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
    logging.info(f"Sending messages to SQS queue {SYNC_LANDSAT_CONNECTION_SQS_QUEUE}")

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


def retrieve_stac_from_api(display_ids):
    """
    Function to create Python threads which will request the API simultaneously

    :param display_ids: (list) id list from the bulk CSV file
    :return:
    """

    logging.info(f"Requesting USGS API")

    # Limit number of threads
    num_of_threads = 50
    logging.info(
        f"Requesting URL {USGS_API_INDIVIDUAL_ITEM_URL} adding the display id by the end of the url"
    )

    with ThreadPoolExecutor(max_workers=num_of_threads) as executor:
        tasks = [
            executor.submit(request_url, f"{USGS_API_INDIVIDUAL_ITEM_URL}/{display_id}")
            for display_id in display_ids
        ]

        for future in as_completed(tasks):
            if future.result():
                yield future.result()


def filter_africa_location_from_gzip_file(file_path: str, production_date: str):
    """
    Function to filter just the Africa location based on the WRS Path and WRS Row. All allowed positions are
    informed through the global variable ALLOWED_PATHROWS which is created when this script file is loaded.
    The function also applies filters to skip LANDSAT_4 and Night shots.
    Warning: This function requires high performance from the CPU.

    :param file_path: (String) Downloaded GZIP file path
    :param production_date: (String) Filter for L2 Product Generation Date ("YYYY/MM/DD" or "YYYY-MM-DD")
    :return: (List) List of Display ids which will be used to retrieve the data from the API.
    """
    logging.info("Start Filtering Scenes by Africa location, Just day scenes and date")

    logging.info(f"Unzipping and filtering file according to Africa Pathrows")

    production_date = production_date.replace("-", "/")

    # Download updated Pathrows
    africa_pathrows = read_csv_from_gzip(file_path=AFRICA_GZ_PATHROWS_URL)

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
                and int(f"{row['WRS Path']}{row['WRS Row']}") in africa_pathrows
            )
        ):
            yield row["Display ID"]


# def filter_africa_location_from_gzip_file(file_path: Path):
#     """
#     Function to filter just the Africa location based on the WRS Path and WRS Row. All allowed positions are
#     informed through the global variable ALLOWED_PATHROWS which is created when this script file is loaded.
#     The function also applies filters to skip LANDSAT_4 and Night shots.
#     Warning: This function requires high performance from the CPU.
#
#     :param file_path: (Path) Downloaded GZIP file path
#     :return: (List) List of Display ids which will be used to retrieve the data from the API.
#     """
#
#     # Get value of last date from Airflow
#     saved_last_date = (
#         convert_str_to_date(Variable.get("landsat_sync_last_date"))
#         if Variable.get("landsat_sync_last_date", default_var=False)
#         else ""
#     )
#
#     # Download updated Pathrows
#     africa_pathrows = read_csv_from_gzip(file_path=AFRICA_GZ_PATHROWS_URL)
#
#     logging.info(
#         f"Unzipping and filtering file according to Africa Pathrows, "
#         f"day scenes and date {saved_last_date if saved_last_date else ''}"
#     )
#
#     # This variable will update airflow variable,
#     # in case of by the end of this process the system finds a higher date
#     last_date = None
#     for row in read_big_csv_files_from_gzip(file_path):
#
#         generated_date = convert_str_to_date(row["Date Product Generated L2"])
#
#         # Update last_date to the latest date in the file
#         if not last_date or generated_date > last_date:
#             last_date = generated_date
#             logging.info(
#                 f"Airflow variable landsat_sync_last_date will be updated to {last_date}"
#             )
#
#         if (
#             row.get("Satellite")
#             and row["Satellite"] != "LANDSAT_4"
#             # Filter to get just day
#             and (
#                 row.get("Day/Night Indicator")
#                 and row["Day/Night Indicator"].upper() == "DAY"
#             )
#             # Filter by the generated date comparing to the last Airflow interaction
#             and (not saved_last_date or saved_last_date < generated_date)
#             # Filter to get just from Africa
#             and (
#                 row.get("WRS Path")
#                 and row.get("WRS Row")
#                 and (int(f"{row['WRS Path']}{row['WRS Row']}") in africa_pathrows)
#             )
#         ):
#             yield row["Display ID"]
#
#     if not saved_last_date or saved_last_date < last_date:
#         logging.info(f"Updating Airflow variable to {last_date}")
#         Variable.set("landsat_sync_last_date", last_date)


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

        if file_path:

            # Read file and retrieve the Display ids
            display_id_list = filter_africa_location_from_gzip_file(
                file_path=file_path, production_date=date_to_process
            )

            # display_id_list = filter_africa_location_from_gzip_file(file_path=file_path)

            if display_id_list:
                # request the API through the display id and send the information to the queue
                stac_list = retrieve_stac_from_api(display_ids=display_id_list)

                # Publish stac to the queue
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
