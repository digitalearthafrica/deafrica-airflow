"""
# Generate a gap report between deafrica-landsat-dev and usgs-landsat buckets

This DAG runs weekly and creates a gap report in the folowing location:
s3://deafrica-landsat-dev/<date>/status-report
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from infra.connections import (
    SYNC_LANDSAT_INVENTORY_ID,
)
from infra.s3_buckets import LANDSAT_SYNC_INVENTORY_BUCKET, LANDSAT_SYNC_S3_BUCKET_NAME
from infra.variables import (
    AWS_DEFAULT_REGION,
    LANDSAT_SYNC_S3_C2_FOLDER_NAME,
)
from landsat_scenes_sync.variables import (
    MANIFEST_SUFFIX,
    BASE_BULK_CSV_URL,
    USGS_API_INDIVIDUAL_ITEM_URL,
    USGS_INDEX_URL,
    AFRICA_GZ_PATHROWS_URL,
)
from utils.inventory import InventoryUtils
from utils.sync_utils import (
    read_csv_from_gzip,
    read_big_csv_files_from_gzip,
    download_file_to_tmp,
    time_process,
    request_url,
)

REPORTING_PREFIX = "status-report/"
SCHEDULE_INTERVAL = "@weekly"

default_args = {
    "owner": "rodrigo.carvalho",
    "start_date": datetime(2021, 3, 29),
    "email": ["rodrigo.carvalho@ga.gov.au", "alex.leith@ga.gov.au"],
    "email_on_failure": True,
    "email_on_success": True,
    "email_on_retry": False,
    "retries": 0,
    "version": "0.0.1",
}


def get_and_filter_keys_from_files(file_path: Path):
    """
    Read scenes from the bulk GZ file and filter
    :param file_path:
    :return:
    """

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
            # Filter to get just from Africa
            and (
                row.get("WRS Path")
                and row.get("WRS Row")
                and int(f"{row['WRS Path']}{row['WRS Row']}") in africa_pathrows
            )
        ):
            # Create name as it's stored in the S3 bucket, so it can be compared
            yield f'{row["Display ID"]}_stac.json'


def get_and_filter_keys(s3_bucket_client):
    """
    Retrieve key list from a inventory bucket and filter
    :param s3_bucket_client:
    :return:
    """

    list_keys = s3_bucket_client.retrieve_keys_from_inventory(
        manifest_sufix=MANIFEST_SUFFIX
    )

    return set(
        key.split("/")[-1]
        for key in list_keys
        if (
            "_stac.json" in key
            # Filter to remove inventory folder or any other despite LANDSAT_SYNC_S3_C2_FOLDER_NAME
            and key.startswith(LANDSAT_SYNC_S3_C2_FOLDER_NAME)
        )
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


def generate_buckets_diff(land_sat: str, file_name: str):
    """
    Compare USGS bulk files and Africa inventory bucket detecting differences
    A report containing missing keys will be written to AFRICA_S3_BUCKET_PATH
    """
    try:
        start_timer = time.time()

        # Download bulk file
        file_path = download_file_to_tmp(url=BASE_BULK_CSV_URL, file_name=file_name)

        # Retrieve keys from the bulk file
        source_keys = get_and_filter_keys_from_files(file_path)

        # Create connection to the inventory S3 bucket
        s3_inventory_dest = InventoryUtils(
            conn=SYNC_LANDSAT_INVENTORY_ID,
            bucket_name=LANDSAT_SYNC_INVENTORY_BUCKET,
            region=AWS_DEFAULT_REGION,
        )

        # Retrieve keys from inventory bucket
        dest_keys = get_and_filter_keys(s3_bucket_client=s3_inventory_dest)

        # Keys that are missing, they are in the source but not in the bucket
        missing_keys = set(key for key in source_keys if key not in dest_keys)

        logging.info(f"missing_keys 10 first keys {list(missing_keys)[0:10]}")

        # Keys that are lost, they are in the bucket but not found in the files
        orphaned_keys = dest_keys.difference(source_keys)
        logging.info(f"orphaned_keys 10 first keys {list(orphaned_keys)[0:10]}")
        # logging.info(f"COMPARING {[k for k in orphaned_keys if k not in missing_keys]}")

        # Build missing scenes links
        # TODO FIX the link to point to the file
        # USGS API URL
        missing_scenes = [
            f"{USGS_API_INDIVIDUAL_ITEM_URL}/{key}" for key in missing_keys
        ]
        logging.info(f"missing_scenes1 : {len(missing_scenes)}")

        # S3 path
        missing_scenes = [
            f"s3://{path}"
            for path in build_s3_url_from_api_metadata(display_ids=missing_keys)
        ]
        logging.info(f"missing_scenes2 : {len(missing_scenes)}")

        output_filename = f"{land_sat}_{datetime.today().isoformat()}.txt"
        key = REPORTING_PREFIX + output_filename

        # Store report in the S3 bucket
        # s3_report = S3(conn_id=SYNC_LANDSAT_CONNECTION_ID)
        #
        # s3_report.put_object(
        #     bucket_name=LANDSAT_SYNC_S3_BUCKET_NAME,
        #     key=key,
        #     region=AWS_DEFAULT_REGION,
        #     body="\n".join(missing_scenes),
        # )

        logging.info(f"Number of missing scenes: {len(missing_scenes)}")
        logging.info(f"Wrote missing scenes to: {LANDSAT_SYNC_S3_BUCKET_NAME}/{key}")

        if len(orphaned_keys) > 0:
            output_filename = f"{land_sat}_{datetime.today().isoformat()}_orphaned.txt"
            key = REPORTING_PREFIX + output_filename
            # s3_report.put_object(
            #     bucket_name=LANDSAT_SYNC_S3_BUCKET_NAME,
            #     key=key,
            #     region=AWS_DEFAULT_REGION,
            #     body="\n".join(orphaned_keys),
            # )
            logging.info(f"Number of orphaned scenes: {len(orphaned_keys)}")
            logging.info(
                f"Wrote orphaned scenes to: {LANDSAT_SYNC_S3_BUCKET_NAME}/{key}"
            )

        message = (
            f"{len(missing_keys)} scenes are missing from {LANDSAT_SYNC_S3_BUCKET_NAME} and {len(orphaned_keys)} "
            f"scenes no longer exist in USGS"
        )
        logging.info(message)
        # if len(missing_keys) > 200 or len(orphaned_keys) > 200:
        #     raise AirflowException(message)

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
        "Landsat_4_5": "LANDSAT_TM_C2_L2.csv.gz",
    }

    for sat, file in files.items():
        processes.append(
            PythonOperator(
                task_id=f"{sat}_compare_s3_inventories",
                python_callable=generate_buckets_diff,
                op_kwargs=dict(land_sat=sat, file_name=file),
            )
        )

    END = DummyOperator(task_id="end-tasks")

    START >> processes >> END
