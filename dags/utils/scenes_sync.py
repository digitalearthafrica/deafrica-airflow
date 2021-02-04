"""
    Script to sync Africa scenes from bulk gzip files or API JSON date request
"""
import gzip
import json
import logging
import threading

import pandas as pd
import requests
from airflow.contrib.hooks.aws_sqs_hook import SQSHook

# ######### AWS CONFIG ############
AWS_CONFIG = {
    'africa_dev_conn_id': 'conn_sync_landsat_scene',
    'sqs_queue': 'deafrica-dev-eks-sync-landsat-scene',
    'arn': 'arn:aws:sqs:ap-southeast-2:717690029437:Rodrigo_Test'
}

# ######### S3 CONFIG ############
SRC_BUCKET_NAME = "sentinel-cogs"
QUEUE_NAME = "deafrica-prod-eks-sentinel-2-data-transfer"

ALLOWED_PATHROWS = [
    int(row.values[0][0])
    for row in pd.read_csv(
        'https://github.com/digitalearthafrica/deafrica-extent/blob/master/deafrica-usgs-pathrows.csv.gz?raw=true',
        compression='gzip',
        header=0,
        chunksize=1
    )
]


# https://github.com/digitalearthafrica/deafrica-extent/blob/master/deafrica-usgs-pathrows.csv.gz

# def get_contents_and_attributes(hook, s3_filepath):
#     bucket_name, key = hook.parse_s3_url(s3_filepath)
#     contents = hook.read_key(key=key, bucket_name=SRC_BUCKET_NAME)
#     contents_dict = json.loads(contents)
#     attributes = get_common_message_attributes(contents_dict)
#     return contents, attributes

def publish_messages(datasets):
    """
    Publish messages
    param message: list of messages
    """
    try:

        def post_messages(messages_to_send):
            queue.send_messages(Entries=messages_to_send)

        sqs_hook = SQSHook(aws_conn_id=AWS_CONFIG["africa_dev_conn_id"])
        sqs = sqs_hook.get_resource_type("sqs")
        queue = sqs.get_queue_by_name(QueueName=AWS_CONFIG["sqs_queue"])

        count = 0
        messages = []
        logging.info("Adding messages...")
        for dataset in datasets:
            message = {
                "Id": str(count),
                "MessageBody": json.dumps(dataset),
            }
            messages.append(message)

            count += 1
            if count % 10 == 0:
                post_messages(messages)
                messages = []

        # Post the last messages if there are any
        if len(messages) > 0:
            post_messages(messages)

        return count

    except Exception as error:
        logging.error(error)
        raise error


def test_http_return(returned):
    """
        Test API response
        :param returned:
        :return:
    """
    if hasattr(returned, 'status_code') and returned.status_code != 200:
        # TODO Implement dead queue here
        url = returned.url if hasattr(returned, 'url') else 'Not informed'
        content = returned.content if hasattr(returned, 'content') else 'Not informed'
        text = returned.text if hasattr(returned, 'text') else 'Not informed'
        status_code = returned.status_code if hasattr(returned, 'status_code') else 'Not informed'
        reason = returned.reason if hasattr(returned, 'reason') else 'Not informed'
        raise Exception(
            f'API return is not 200: \n'
            f'-url: {url} \n'
            f'-content: {content} \n'
            f'-text: {text} \n'
            f'-status_code: {status_code} \n'
            f'-reason: {reason} \n'
        )


def get_allowed_features_json(retrieved_json):
    """
        Function to filter the scenes and allow just the Africa ones
        :param retrieved_json: (dict) retrieved value
        :return: (list)
    """
    try:
        if retrieved_json.get('features') and retrieved_json['features']:
            return [
                feature
                for feature in retrieved_json['features']
                if (
                        feature.get('properties')
                        and feature['properties'].get('landsat:wrs_path')
                        and int(f"{feature['properties']['landsat:wrs_path']}"
                                f"{feature['properties']['landsat:wrs_row']}") in ALLOWED_PATHROWS
                )
            ]
        elif (
                retrieved_json
                and retrieved_json.get('properties')
                and retrieved_json['properties'].get('landsat:wrs_path')
                and retrieved_json['properties'].get('landsat:wrs_row')
        ):
            if int(
                    f"{retrieved_json['properties']['landsat:wrs_path']}"
                    f"{retrieved_json['properties']['landsat:wrs_row']}"
            ) in ALLOWED_PATHROWS:
                return retrieved_json
        elif not retrieved_json.get('features'):
            return [retrieved_json]

        return []
    except Exception as error:
        raise error


def validate_and_send(api_return):
    """
        Validates pathrow and, when valid, send returned value to the queue
        :param api_return: (list) list of values returned from the API
        :return:
    """
    try:
        datasets = get_allowed_features_json(retrieved_json=api_return)
        if datasets:
            publish_messages(datasets=datasets)

    except Exception as error:
        raise error


def request_api_and_send(url: str, params=None):
    """
        Function to request the API and send the returned value to the queue.
        If parameters are sent, means we are requesting daily JSON API, or part of its recursion
        If parameters are empty, means we are using bulk CSV files to retrieve the information

        :param url: (String) API URL
        :param params: (Dict) Parameters to add to the URL
        :return: None
    """
    try:
        if params is None:
            params = {}

        # Request API
        resp = requests.get(url=url, params=params)
        # Check return 200
        test_http_return(resp)
        returned = json.loads(resp.content)

        # Retrieve daily requests
        if params:

            logging.debug(f"Found {returned['meta']['found']}")

            # TODO to speed up the process, it's possible to create threads to execute the send function at this point
            validate_and_send(api_return=returned)

            if (
                    returned.get('meta')
                    and returned['meta'].get('page')
                    and returned['meta'].get('limit')
                    and returned['meta'].get('found')
                    and returned['meta'].get('returned')
            ):
                if (
                        returned['meta']['returned'] == returned['meta']['limit']
                        and (returned['meta']['page'] * returned['meta']['limit']) < returned['meta']['found']
                ):
                    params.update({'page': returned['meta']['page'] + 1})
                    request_api_and_send(url=url, params=params)

        else:
            # Came from the bulk CSV file
            validate_and_send(api_return=returned)
    except Exception as error:
        raise error


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
    # https://landsatlook.usgs.gov/sat-api/stac/search?collection=landsat-c2l2-sr&time=2020-12-07
    # https://landsatlook.usgs.gov/sat-api/collections/landsat-c2l2-sr/items/LC08_L1GT_166112_20210123_20210123_02_RT

    try:

        if not date and not display_ids:
            raise Exception(
                'Date is required for daily JSON request. '
                'For a bulk CSV request, Display_ids is required'
            )

        main_url = 'https://landsatlook.usgs.gov/sat-api'
        africa_bbox = [-26.359944882003788, -47.96476498374171, 64.4936701740102, 38.34459242512347]

        if not display_ids:
            json_url = f'{main_url}/stac/search'
            params = {
                    'collection': 'landsat-c2l2-sr',
                    'limit': 100,
                    'bbox': json.dumps(africa_bbox),
                    'time': date.date().isoformat()
                }

            logging.info(f'Requesting URL {json_url} with parameters {params}')

            # Request daily JSON API
            request_api_and_send(url=json_url, params=params)

        else:
            # Limit number of threads
            num_of_threads = 16
            count_tasks = len(display_ids)
            id_request_url = f'{main_url}/collections/landsat-c2l2-sr/items/'
            logging.info(
                f'Simultaneously {num_of_threads} process/requests (Python threads) to send {count_tasks} messages'
            )
            logging.info(f'Requesting URL {id_request_url} adding the display id by the end of the url')

            while count_tasks > 0:

                if count_tasks < num_of_threads:
                    num_of_threads = count_tasks

                # Create threads for the ids from the bulk CSV file
                thread_list = [
                    threading.Thread(
                        target=request_api_and_send,
                        args=(f'{id_request_url}{display_id}',)
                    )
                    for display_id in display_ids[(count_tasks - num_of_threads):count_tasks]
                ]

                # Start Threads
                [start_thread.start() for start_thread in thread_list]

                logging.info('Running threads to retrieve and send '.format(number=count_tasks))
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
    try:
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
            values = str(bvalue).rstrip('\n').split(',')
            if not built_header:
                return values
            else:
                return {header[index]: values[index] for index in range(0, len(header))}

        for row in gzip.open(file_path, 'rt'):
            # Gzip library does not skip header line, instead reads line by line returning a byte string,
            # so the inner-function will build the right dict based on the header
            if not header:
                header = build_dict(header, row)
            else:
                yield build_dict(header, row)

    except Exception as error:
        raise error


def filter_africa_location(file_path):
    """
        Function to filter just the Africa location based on the WRS Path and WRS Row. All allowed positions are
        informed through the global variable ALLOWED_PATHROWS which is created when this script file is loaded.
        :param file_path: (String) Downloaded GZIP file path
        :return: (List) List of Display ids which will be used to retrieve the data from the API.
    """
    try:
        logging.info(f"Unzipping and filtering file according to Africa Pathrows")
        return [
            row['Display ID']
            for row in read_csv(file_path)
            if (
                    not row.get('WRS Path')
                    or not row.get('WRS Row')
                    or not int('{path}{row}'.format(path=row['WRS Path'], row=row['WRS Row'])) in ALLOWED_PATHROWS
            )
        ]
    except Exception as error:
        raise error


def download_csv_files(url, file_name):
    """
        Function to download bulk CSV file from the informed server.

        :param url:(String) URL path for the API
        :param file_name: (String) File name which will be downloaded
        :return: (String) File path where it was downloaded. Hardcoded for /tmp/
    """
    try:
        url = f'{url}{file_name}'

        file_path = f'/tmp/{file_name}'
        with open(file_path, "wb") as f:
            logging.info(f"Downloading file {file_name} to {file_path}")
            downloaded = requests.get(url, stream=True)
            f.write(downloaded.content)

        # ########## Code to add downloading progress bar ###########
        # with open(file_path, "wb") as f:
        #     print(f"Downloading {file_name}")
        #     downloaded = requests.get(url, stream=True)
        #     total_length = downloaded.headers.get('content-length')
        #
        #     # Percentage bar to show download progress
        #     if total_length is None:  # no content length header
        #         f.write(downloaded.content)
        #     else:
        #         dl = 0
        #         total_length = int(total_length)
        #         for data in downloaded.iter_content(chunk_size=4096):
        #             dl += len(data)
        #             f.write(data)
        #             done = int(50 * dl / total_length)
        #             sys.stdout.write("\r[{0}{1}]".format('=' * done, ' ' * (50 - done)))
        #             sys.stdout.flush()

        logging.info(f"{file_name} Downloaded!")
        return file_path
    except Exception as error:
        raise error


def retrieve_bulk_data(file_name):
    """
        Function to initiate the bulk CSV process
        :param file_name: (String) File name which will be downloaded
        :return: None. Process send information to the queue
    """
    try:
        # Main URL
        main_url = 'https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/'

        # Download GZIP file
        file_path = download_csv_files(main_url, file_name)

        # Read file and retrieve the Display ids
        display_id_list = filter_africa_location(file_path)
        logging.info(f"{len(display_id_list)} found after being filtered")

        # request the API through the display id and send the information to the queue
        retrieve_json_data_and_send(display_ids=display_id_list)

    except Exception as error:
        logging.error(error)
        raise error


if __name__ == "__main__":

    # retrieve_json_data_and_send(date=datetime.now().replace(day=28, month=1, year=2021))

    # 'https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/LANDSAT_OT_C2_L1.csv.gz'
    # 'https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/LANDSAT_OT_C2_L2.csv.gz'
    # 'https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/LANDSAT_ETM_C2_L1.csv.gz'
    # https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/LANDSAT_ETM_C2_L2.csv.gz
    # https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/LANDSAT_TM_C2_L2.csv.gz

    # TODO start DAG here to have each process downloading
    # TODO add all files just level 2, ignore Landsat 4
    files = {
        'landsat_8': 'LANDSAT_OT_C2_L2.csv.gz',
        'landsat_7': 'LANDSAT_ETM_C2_L2.csv.gz',
        'Landsat_4_5': 'LANDSAT_TM_C2_L2.csv.gz'
    }

    for sat, file in files.items():
        retrieve_bulk_data(file_name=file)
