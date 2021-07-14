"""
# Generate a gap report between deafrica-landsat-dev and usgs-landsat buckets

This DAG runs weekly and creates a gap report in the folowing location:
s3://deafrica-landsat-dev/<date>/status-report
"""

import logging
import time
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed
)
from datetime import datetime
from pathlib import Path

from airflow import (
    DAG,
    AirflowException
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from infra.connections import (
    CONN_LANDSAT_SYNC,
    CONN_LANDSAT_WRITE
)
from infra.s3_buckets import (
    LANDSAT_INVENTORY_BUCKET_NAME,
    LANDSAT_SYNC_BUCKET_NAME
)
from infra.variables import (
    REGION,
)
from landsat_scenes_sync.variables import (
    MANIFEST_SUFFIX,
    BASE_BULK_CSV_URL,
    USGS_API_INDIVIDUAL_ITEM_URL,
    USGS_INDEX_URL,
    AFRICA_GZ_PATHROWS_URL,
    USGS_S3_BUCKET_PATH,
    AFRICA_S3_BUCKET_PATH,
    C2_FOLDER_NAME,
)
from utils.aws_utils import S3
from utils.inventory import InventoryUtils
from utils.sync_utils import (
    read_csv_from_gzip,
    read_big_csv_files_from_gzip,
    download_file_to_tmp,
    time_process,
    request_url,
    convert_str_to_date,
)

REPORTING_PREFIX = "status-report/"
# Dev does not need to be updated
SCHEDULE_INTERVAL = None

default_args = {
    "owner": "RODRIGO",
    "start_date": datetime(2021, 3, 29),
    "email": ["systems@digitalearthafrica.org"],
    "email_on_failure": True,
    "email_on_success": True,
    "email_on_retry": False,
    "retries": 0,
    "version": "0.0.3",
}


def get_and_filter_keys_from_files(file_path: Path):
    """
    Read scenes from the bulk GZ file and filter
    :param file_path:
    :return:
    """

    def build_path(file_row):
        # USGS changes - for _ when generates the CSV bulk file
        identifier = file_row["Sensor Identifier"].lower().replace("_", "-")
        year_acquired = convert_str_to_date(file_row["Date Acquired"]).year

        return (
            "collection02/level-2/standard/{identifier}/{year_acquired}/"
            "{target_path}/{target_row}/{display_id}/".format(
                identifier=identifier,
                year_acquired=year_acquired,
                target_path=file_row["WRS Path"].zfill(3),
                target_row=file_row["WRS Row"].zfill(3),
                display_id=file_row["Display ID"],
            )
        )

    # Download updated Pathrows
    logging.info(f"Retrieving allowed Africa Pathrows from {AFRICA_GZ_PATHROWS_URL}")
    africa_pathrows = read_csv_from_gzip(file_path=AFRICA_GZ_PATHROWS_URL)

    logging.info("Reading and filtering Bulk file")

    return set(
        build_path(row)
        for row in read_big_csv_files_from_gzip(file_path)
        if (
            # Filter to skip all LANDSAT_4
            row.get("Satellite")
            and row["Satellite"] != "LANDSAT_4"
            # Filter to get just day
            and (
                row.get("Day/Night Indicator")
                and row["Day/Night Indicator"].upper() == "DAY"
            )
            # Filter to get just from Africa
            and (
                row.get("WRS Path")
                and row.get("WRS Row")
                and int(f"{row['WRS Path'].zfill(3)}{row['WRS Row'].zfill(3)}")
                in africa_pathrows
            )
        )
    )


def get_and_filter_keys(s3_bucket_client, landsat: str):
    """
    Retrieve key list from a inventory bucket and filter
    :param s3_bucket_client:
    :param landsat:(str)
    :return:
    """

    sat_prefix = None
    if landsat == "landsat_8":
        sat_prefix = "LC08"
    elif landsat == "landsat_7":
        sat_prefix = "LE07"
    elif landsat == "Landsat_5":
        sat_prefix = "LT05"

    if not sat_prefix:
        raise Exception(f"prefix not defined")

    list_json_keys = s3_bucket_client.retrieve_keys_from_inventory(
        manifest_sufix=MANIFEST_SUFFIX,
        prefix=C2_FOLDER_NAME,
        suffix='_stac.json',
        contains=sat_prefix
    )

    logging.info(f"Filtering by sat prefix {sat_prefix}")

    return set(
        f"{key.rsplit('/', 1)[0]}/"
        for key in list_json_keys
    )


def build_s3_url_from_api_metadata(display_ids):
    """
    Function to create Python threads which will request the API simultaneously

    :param display_ids: (list) id list from the bulk CSV file
    :return:
    """

    def request_usgs_api(url: str):

        try:
            response = request_url(url=url)
            if response.get("assets"):
                index_asset = response["assets"].get("index")
                if index_asset and hasattr(index_asset, "href"):
                    file_name = index_asset.href.split("/")[-1]
                    asset_s3_path = index_asset.href.replace(USGS_INDEX_URL, "")
                    return f"{asset_s3_path}/{file_name}_SR_stac.json"

        except Exception as error:
            # If the request return an error, just log and keep going
            logging.error(f"Error requesting API: {error}")

    # Limit number of threads
    num_of_threads = 50
    with ThreadPoolExecutor(max_workers=num_of_threads) as executor:
        tasks = []
        for display_id in display_ids:
            tasks.append(
                executor.submit(
                    request_usgs_api, f"{USGS_API_INDIVIDUAL_ITEM_URL}/{display_id}"
                )
            )

        for future in as_completed(tasks):
            if future.result():
                yield future.result()


def generate_buckets_diff(landsat: str, file_name: str):
    """
    Compare USGS bulk files and Africa inventory bucket detecting differences
    A report containing missing keys will be written to AFRICA_S3_BUCKET_PATH
    """
    try:
        start_timer = time.time()

        logging.info("Comparing")

        # Download bulk file
        file_path = download_file_to_tmp(url=BASE_BULK_CSV_URL, file_name=file_name)

        # Retrieve keys from the bulk file
        logging.info("Filtering keys from bulk file")
        source_paths = get_and_filter_keys_from_files(file_path)

        logging.info(f"BULK FILE number of objects {len(source_paths)}")
        logging.info(f"BULK 10 First {list(source_paths)[0:10]}")

        # Create connection to the inventory S3 bucket
        s3_inventory_dest = InventoryUtils(
            conn=CONN_LANDSAT_SYNC,
            bucket_name=LANDSAT_INVENTORY_BUCKET_NAME,
            region=REGION,
        )

        # Retrieve keys from inventory bucket
        logging.info(f"Connecting to inventory bucket {LANDSAT_INVENTORY_BUCKET_NAME}")
        dest_paths = get_and_filter_keys(
            s3_bucket_client=s3_inventory_dest, landsat=landsat
        )

        logging.info(f"INVENTORY bucket number of objects {len(dest_paths)}")
        logging.info(f"INVENTORY 10 first {list(dest_paths)[0:10]}")

        # Keys that are missing, they are in the source but not in the bucket
        logging.info("Filtering missing scenes")
        missing_scenes = [
            f"{USGS_S3_BUCKET_PATH}{path}"
            for path in source_paths.difference(dest_paths)
        ]

        # Keys that are orphan, they are in the bucket but not found in the files
        logging.info("Filtering orphan scenes")
        orphaned_scenes = [
            f"{AFRICA_S3_BUCKET_PATH}{path}"
            for path in dest_paths.difference(source_paths)
        ]

        logging.info(f"missing_scenes 10 first keys {list(missing_scenes)[0:10]}")
        logging.info(f"orphaned_scenes 10 first keys {list(orphaned_scenes)[0:10]}")

        output_filename = f"{landsat}_{datetime.today().isoformat()}.txt"
        key = REPORTING_PREFIX + output_filename

        # Store report in the S3 bucket
        s3_report = S3(conn_id=CONN_LANDSAT_WRITE)

        s3_report.put_object(
            bucket_name=LANDSAT_SYNC_BUCKET_NAME,
            key=key,
            region=REGION,
            body="\n".join(missing_scenes),
        )

        logging.info(f"Number of missing scenes: {len(missing_scenes)}")
        logging.info(f"Wrote missing scenes to: {LANDSAT_SYNC_BUCKET_NAME}/{key}")

        if len(orphaned_scenes) > 0:
            output_filename = f"{landsat}_{datetime.today().isoformat()}_orphaned.txt"
            key = REPORTING_PREFIX + output_filename
            s3_report.put_object(
                bucket_name=LANDSAT_SYNC_BUCKET_NAME,
                key=key,
                region=REGION,
                body="\n".join(orphaned_scenes),
            )
            logging.info(f"Number of orphaned scenes: {len(orphaned_scenes)}")
            logging.info(f"Wrote orphaned scenes to: {LANDSAT_SYNC_BUCKET_NAME}/{key}")

        message = (
            f"{len(missing_scenes)} scenes are missing from {LANDSAT_SYNC_BUCKET_NAME} "
            f"and {len(orphaned_scenes)} scenes no longer exist in USGS"
        )

        if len(missing_scenes) > 200 or len(orphaned_scenes) > 200:
            raise AirflowException(message)

        logging.info(message)

        logging.info(
            f"File {file_name} processed and sent in {time_process(start=start_timer)}"
        )
    except Exception as error:
        logging.error(error)
        raise error


with DAG(
    "landsat_scenes_gap_report",
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
    tags=["Landsat_scenes", "status", "gap_report"],
    catchup=False,
) as dag:
    START = DummyOperator(task_id="start-tasks")

    processes = []
    files = {
        "landsat_8": "LANDSAT_OT_C2_L2.csv.gz",
        "landsat_7": "LANDSAT_ETM_C2_L2.csv.gz",
        "Landsat_5": "LANDSAT_TM_C2_L2.csv.gz",
    }

    for sat, file in files.items():
        processes.append(
            PythonOperator(
                task_id=f"{sat}_compare_s3_inventories",
                python_callable=generate_buckets_diff,
                op_kwargs=dict(landsat=sat, file_name=file),
            )
        )

    END = DummyOperator(task_id="end-tasks")

    START >> processes >> END
