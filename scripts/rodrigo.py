import gzip
import json
import os
import sys
import threading
from datetime import timedelta, datetime

import pandas as pd
import requests


# ######### S3 CONFIG ############
SRC_BUCKET_NAME = "sentinel-cogs"
QUEUE_NAME = "deafrica-prod-eks-sentinel-2-data-transfer"


DATA_DIR = '../data'
ALLOWED_PATHROWS_FILE_NAME = 'deafrica-usgs-pathrows.csv.gz'
ALLOWED_PATHROWS = pd.read_csv(
    os.path.join(DATA_DIR, ALLOWED_PATHROWS_FILE_NAME)
).values.ravel().tolist()


# def get_contents_and_attributes(hook, s3_filepath):
#     bucket_name, key = hook.parse_s3_url(s3_filepath)
#     contents = hook.read_key(key=key, bucket_name=SRC_BUCKET_NAME)
#     contents_dict = json.loads(contents)
#     attributes = get_common_message_attributes(contents_dict)
#     return contents, attributes


def test_http_return(returned):
    """
        Test API response
        :param returned:
        :return:
    """
    if hasattr(returned, 'status_code') and returned.status_code != 200:
        # TODO Implement dead queue here
        raise Exception(f'API return is not 200: {returned}')


def get_allowed_features_json(retrieved_json):
    """
        Function to filter the scenes and allow just the Africa ones
        :param retrieved_json: (dict) retrieved value
        :return: (list)
    """
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
    return []


def send(messages):
    try:
        print('messages sent: {number}'.format(number=len(messages)))
    except Exception as error:
        raise error


def request_api_and_send(url: str, params=None, next_page=False):
    """
        Function to request the API and send the returned value to the queue.
        If parameters are sent, means we are requesting daily JSON API
        If parameters are empty, means we are using bulk CSV files to retrieve the information
        If the next_page parameter is sent, indicates that's part of the a previous request which is
        using pagination, this will repeatedly happen until the last page which won't have the next page link.

        :param next_page:(bool) boolean to indicate that is part of the same request just using pagination.
        :param url: (String) API URL
        :param params: (Dict) Parameters to add to the URL
        :return: None
    """

    if params is None:
        params = {}

    # Request API
    resp = requests.get(url=url, params=params)
    # Check return 200
    test_http_return(resp)
    returned = json.loads(resp.content)

    # Retrieve daily requests
    if params or next_page:
        allowed_list = get_allowed_features_json(retrieved_json=returned)
        if allowed_list:
            send(allowed_list)

        if returned.get('links'):
            for link in returned['links']:
                if link.get('rel') and link['rel'] == "next" and link.get('href'):
                    request_api_and_send(url=link['href'], next_page=True)

    else:
        # Came from the bulk CSV file
        send([returned])


def retrieve_json_data_and_send(start_date=None, end_date=None, display_ids=None):
    """
        Function to create Python threads which will request the API simultaneously
        If start_date and end_date are sent, means we are requesting daily JSON API
        If display_ids is sent, means we are using bulk CSV files to retrieve the information

        :param start_date: (datetime) start range of dates which the system will use to retrieve information
        :param end_date: (datetime) end range of dates which the system will use to retrieve information
        :param display_ids: (list) id list from the bulk CSV file
        :return:
    """
    # https://landsatlook.usgs.gov/sat-api/stac/search?collection=landsat-c2l2-sr&time=2020-12-07
    # https://landsatlook.usgs.gov/sat-api/collections/landsat-c2l2-sr/items/LC08_L1GT_166112_20210123_20210123_02_RT

    try:
        if not display_ids:
            if start_date and not end_date:
                end_date = datetime.now()
            elif end_date and not start_date:
                start_date = datetime.now()

        if (start_date and end_date) or display_ids:
            main_url = 'https://landsatlook.usgs.gov/sat-api'
            json_url = f'{main_url}/stac/search'
            num_of_threads = 16
            count_tasks = ((end_date - start_date).days + 1) if not display_ids else len(display_ids)
            requested_date = start_date
            africa_bbox = [-26.359944882003788, -47.96476498374171, 64.4936701740102, 38.34459242512347]

            while count_tasks > 0:

                if count_tasks < num_of_threads:
                    num_of_threads = count_tasks

                if not display_ids:
                    print(f'num threads {num_of_threads}')
                    print(f'count_tasks {count_tasks}')
                    print(f'start_date {start_date}')
                    print(f'end_date {end_date}')
                    # Create threads for the daily JSON request
                    thread_list = [
                        threading.Thread(
                            target=request_api_and_send,
                            args=(
                                json_url,
                                {
                                    'collection': 'landsat-c2l2-sr',
                                    'limit': 100,
                                    'bbox': json.dumps(africa_bbox),
                                    'time': (requested_date + timedelta(days=thread)).date().isoformat()
                                }
                            )
                        ) for thread in range(num_of_threads)
                    ]

                    requested_date += timedelta(days=num_of_threads)

                else:

                    # Create threads for the ids from the bulk CSV file
                    thread_list = [
                        threading.Thread(
                            target=request_api_and_send,
                            args=(f'{main_url}/collections/landsat-c2l2-sr/items/{display_id}',)
                        )
                        for display_id in display_ids[count_tasks:(count_tasks + num_of_threads)]
                    ]

                # Start Threads
                [start_thread.start() for start_thread in thread_list]

                # Wait all {num_of_threads} threads finish to start {num_of_threads} more
                [join_thread.join() for join_thread in thread_list]

                count_tasks -= num_of_threads
        else:
            raise Exception(
                'Start_date and End_date are required for daily JSON request. '
                'For a bulk CSV request, Display_ids is required'
            )

    except Exception as error:
        print(error)
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
            print(f"Downloading {file_name}")
            downloaded = requests.get(url, stream=True)
            total_length = downloaded.headers.get('content-length')

            # Percentage bar to show download progress
            if total_length is None:  # no content length header
                f.write(downloaded.content)
            else:
                dl = 0
                total_length = int(total_length)
                for data in downloaded.iter_content(chunk_size=4096):
                    dl += len(data)
                    f.write(data)
                    done = int(50 * dl / total_length)
                    sys.stdout.write("\r[{0}{1}]".format('=' * done, ' ' * (50 - done)))
                    sys.stdout.flush()

        return file_path
    except Exception as error:
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

        # request the API through the display id and send the information to the queue
        retrieve_json_data_and_send(display_ids=display_id_list)

    except Exception as error:
        print(error)
        raise error


if __name__ == "__main__":
    retrieve_json_data_and_send(start_date=datetime.now().replace(day=28, month=1, year=2021), end_date=datetime.now())

    # 'https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/LANDSAT_OT_C2_L1.csv.gz'
    # 'https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/LANDSAT_OT_C2_L2.csv.gz'
    # 'https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/LANDSAT_ETM_C2_L1.csv.gz'
    # https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/LANDSAT_ETM_C2_L2.csv.gz
    # https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/LANDSAT_TM_C2_L2.csv.gz

    # TODO start DAG here to have each process downloading
    # TODO add all files just level 2, ignore Landsat 4
    # files = {
    #     'landsat_8': 'LANDSAT_OT_C2_L2.csv.gz',
    #     'landsat_7': 'LANDSAT_ETM_C2_L2.csv.gz',
    #     'Landsat_4_5': 'LANDSAT_TM_C2_L2.csv.gz'
    # }
    #
    # for sat, file in files.items():
    #     retrieve_bulk_data(file_name=file)
