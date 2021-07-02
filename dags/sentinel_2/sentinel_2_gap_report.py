"""
# Generate a gap report between sentinel-cogs and deafica-sentinel-2 buckets

This DAG runs once a month and creates a gap report in the folowing location:
s3://deafrica-sentinel-2/status-report
"""
import logging
import re
from datetime import datetime

from airflow import DAG, AirflowException
from airflow.operators.python_operator import PythonOperator

from infra.connections import CONN_SENTINEL_2_SYNC
from infra.s3_buckets import (
    SENTINEL_2_INVENTORY_BUCKET_NAME,
    SENTINEL_2_SYNC_BUCKET_NAME,
)
from infra.variables import REGION
from landsat_scenes_sync.variables import MANIFEST_SUFFIX, USGS_AWS_REGION
from sentinel_2.variables import (
    AFRICA_TILES,
    REPORTING_PREFIX,
    SENTINEL_COGS_INVENTORY_BUCKET,
    SENTINEL_2_S3_COGS_FOLDER_NAME
)
from utils.inventory import InventoryUtils
from utils.sync_utils import read_csv_from_gzip

default_args = {
    "owner": "Rodrigo Carvalho",
    "start_date": datetime(2020, 7, 24),
    "email": ["systems@digitalearthafrica.org"],
    "email_on_failure": True,
    "email_on_success": True,
    "email_on_retry": False,
    "retries": 0,
}


def get_and_filter_source_keys(s3_bucket_client):
    """
    Retrieve key list from a inventory bucket and filter
    :param s3_bucket_client:
    :return:
    """

    logging.info("Gathering and filtering source Keys")
    list_keys = s3_bucket_client.retrieve_keys_from_inventory(
        manifest_sufix=MANIFEST_SUFFIX
    )
    logging.info("Reading Africa tiles")
    africa_tile_ids = read_csv_from_gzip(file_path=AFRICA_TILES)

    return set(
        key
        for key in list_keys
        if (
            ".json" in key
            and key.startswith(SENTINEL_2_S3_COGS_FOLDER_NAME)
            and key.split("/")[-2].split("_")[1] in africa_tile_ids
            # We need to ensure we're ignoring the old format data
            and re.match(r"sentinel-s2-l2a-cogs/\d{4}/", key) is None
        )
    )


def get_and_filter_destination_keys(s3_bucket_client):
    """
    Retrieve key list from a inventory bucket and filter
    :param s3_bucket_client:
    :return:
    """
    logging.info("Gathering and filtering destination Keys")
    list_keys = s3_bucket_client.retrieve_keys_from_inventory(
        manifest_sufix=MANIFEST_SUFFIX
    )
    logging.info("Filtering")
    return set(
        key
        for key in list_keys
        if ".json" in key and key.startswith(SENTINEL_2_S3_COGS_FOLDER_NAME)
    )


def generate_buckets_diff():
    """
    Compare Sentinel-2 buckets in US and Africa and detect differences
    A report containing missing keys will be written to s3://deafrica-sentinel-2/status-report
    """
    logging.info("Process started")
    # Create connection to the inventory S3 bucket
    s3_inventory_source = InventoryUtils(
        conn=CONN_SENTINEL_2_SYNC,
        bucket_name=SENTINEL_COGS_INVENTORY_BUCKET,
        # region=REGION,
        region=USGS_AWS_REGION,
    )
    logging.info(
        f"Connected to S3 source {SENTINEL_COGS_INVENTORY_BUCKET} - {REGION}"
    )

    # Retrieve keys from inventory bucket
    source_keys = get_and_filter_source_keys(s3_bucket_client=s3_inventory_source)

    # Create connection to the inventory S3 bucket
    s3_inventory_destination = InventoryUtils(
        conn=CONN_SENTINEL_2_SYNC,
        bucket_name=SENTINEL_2_INVENTORY_BUCKET_NAME,
        region=REGION,
    )
    logging.info(f"Connected to S3 destination {SENTINEL_2_INVENTORY_BUCKET_NAME}")

    # Retrieve keys from inventory bucket
    destination_keys = get_and_filter_destination_keys(
        s3_bucket_client=s3_inventory_destination
    )

    # Keys that are missing, they are in the source but not in the bucket
    missing_scenes = set(
        f"s3://sentinel-cogs/{key}"
        for key in source_keys
        if key not in destination_keys
    )

    # Keys that are lost, they are in the bucket but not found in the files
    orphaned_keys = destination_keys.difference(source_keys)

    output_filename = datetime.today().isoformat() + ".txt"
    key = REPORTING_PREFIX + output_filename

    # Store report in the S3 bucket
    # s3_report = S3(conn_id=CONN_SENTINEL_2_SYNC)
    #
    # s3_report.put_object(
    #     bucket_name=SENTINEL_2_SYNC_BUCKET_NAME,
    #     key=key,
    #     region=REGION,
    #     body="\n".join(missing_scenes),
    # )
    logging.info(
        f"missing_scenes {missing_scenes if len(missing_scenes) < 100 else list(missing_scenes)[0:2]}"
    )
    logging.info(f"10 first missing_scenes {list(missing_scenes)[0:10]}")
    print(f"Wrote inventory to: s3://{SENTINEL_2_SYNC_BUCKET_NAME}/{key}")

    if len(orphaned_keys) > 0:
        output_filename = datetime.today().isoformat() + "_orphaned.txt"
        key = REPORTING_PREFIX + output_filename
        # s3_report.put_object(
        #     bucket_name=SENTINEL_2_SYNC_BUCKET_NAME,
        #     key=key,
        #     region=REGION,
        #     body="\n".join(orphaned_keys),
        # )

        logging.info(f"10 first orphaned_keys {orphaned_keys[0:10]}")

        print(f"Wrote orphaned scenes to: s3://{SENTINEL_2_SYNC_BUCKET_NAME}/{key}")

    message = (
        f"{len(missing_scenes)} scenes are missing from "
        f"s3://{SENTINEL_2_SYNC_BUCKET_NAME} and {len(orphaned_keys)} "
        f"scenes no longer exist in s3://sentinel-cogs"
    )
    print(message)

    if len(missing_scenes) > 200 or len(orphaned_keys) > 200:
        raise AirflowException(message)


with DAG(
    "sentinel-2_gap_detection",
    default_args=default_args,
    # DEV does not need to be updated
    schedule_interval=None,
    tags=["Sentinel-2", "status"],
    catchup=False,
) as dag:

    READ_INVENTORIES = PythonOperator(
        task_id="compare_s2_inventories", python_callable=generate_buckets_diff
    )
