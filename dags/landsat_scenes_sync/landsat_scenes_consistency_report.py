"""
# Generate a gap report between deafrica-landsat-dev and usgs-landsat buckets

This DAG runs weekly and creates a gap report in the folowing location:
s3://deafrica-landsat-dev/<date>/status-report
"""

from datetime import datetime, timedelta

import re
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from airflow.operators.python_operator import PythonOperator
from airflow import DAG, AirflowException

from infra.connections import (
    SYNC_LANDSAT_CONNECTION_ID,
    MIGRATION_OREGON_CONNECTION_ID,
    SYNC_LANDSAT_INVENTORY_ID,
)
from infra.variables import (
    LANDSAT_SYNC_S3_BUCKET_NAME,
    SENTINEL_2_INVENTORY_BUCKET,
    SENTINEL_2_INVENTORY_UTILS_BUCKET,
    SENTINEL_COGS_INVENTORY_BUCKET,
    AWS_DEFAULT_REGION,
)
from landsat_scenes_sync.variables import USGS_S3_BUCKET_NAME, USGS_AWS_REGION
from utils.aws_utils import S3
from utils.inventory import InventoryUtils

MANIFEST_SUFFIX = "manifest.json"
AFRICA_TILES = "data/africa-mgrs-tiles.csv"
AFRICA_CONN_ID = "deafrica-prod-migration"

REPORTING_PREFIX = "status-report/"
SCHEDULE_INTERVAL = "@weekly"

DEST_BUCKET_NAME = f"s3://{SENTINEL_2_INVENTORY_BUCKET}"
SRC_BUCKET_NAME = f"s3://{SENTINEL_COGS_INVENTORY_BUCKET}"
REPORTING_BUCKET = f"3://{SENTINEL_2_INVENTORY_UTILS_BUCKET}"


default_args = {
    "owner": "rodrigo.carvalho",
    "start_date": datetime(2021, 3, 29),
    "email": ["rodrigo.carvalho@ga.gov.au", "alex.leith@ga.gov.au"],
    "email_on_failure": True,
    "email_on_success": True,
    "email_on_retry": False,
    "retries": 0,
}


def generate_buckets_diff():
    """
    Compare Sentinel-2 buckets in US and Africa and detect differences
    A report containing missing keys will be written to s3://deafrica-sentinel-2/status-report
    """
    cogs_folder_name = "sentinel-s2-l2a-cogs"

    africa_tile_ids = set(
        pd.read_csv(
            "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/deafrica-mgrs-tiles.csv.gz",
            header=None,
        ).values.ravel()
    )

    def list_keys_per_file(s3_bucket_client, file_key):
        to_return = []

        list_keys = s3_bucket_client.list_keys_in_file(key=file_key)
        # logging.info('list_keys starts now')
        for bucket, key, *rest in list_keys:
            if (
                ".json" in key
                and key.startswith(cogs_folder_name)
                and key.split("/")[-2].split("_")[1] in africa_tile_ids
                # We need to ensure we're ignoring the old format data
                and re.match(r"sentinel-s2-l2a-cogs/\d{4}/", key) is None
            ):
                to_return.append(key)

        # logging.info('list_keys Ended')
        return to_return

        # return set(
        #     key for bucket, key, *rest in s3_bucket_client.list_keys_in_file(key=file_key)
        #     if (
        #             ".json" in key
        #             and key.startswith(cogs_folder_name)
        #             and key.split("/")[-2].split("_")[1] in africa_tile_ids
        #             # We need to ensure we're ignoring the old format data
        #             and re.match(r"sentinel-s2-l2a-cogs/\d{4}/", key) is None
        #     )
        # )

    def retrieve_keys_from_inventory_file(s3_bucket_client):
        # Limit number of threads
        num_of_threads = 50
        with ThreadPoolExecutor(max_workers=num_of_threads) as executor:
            logging.info("RETRIEVING AND FILTERING KEYS FROM INVENTORY FILE")

            tasks = [
                executor.submit(
                    list_keys_per_file,
                    s3_bucket_client,
                    file["key"],
                )
                for file in s3_bucket_client.retrieve_manifest_files(
                    suffix="manifest.json"
                )
                if file.get("key")
            ]

            result = []
            count = 0
            for future in as_completed(tasks):
                if future.result():
                    result.extend(future.result())
                    count += 1
            # [
            #     result.extend(future.result()) for future in as_completed(tasks) if future.result()
            # ]

            return set(result)

    s3_inventory = InventoryUtils(
        conn=SYNC_LANDSAT_INVENTORY_ID,
        bucket_name=SENTINEL_COGS_INVENTORY_BUCKET,
        region=USGS_AWS_REGION,
    )
    source_keys = retrieve_keys_from_inventory_file(s3_bucket_client=s3_inventory)

    logging.info(f"source_keys {len(source_keys)}")

    s3_inventory_dest = InventoryUtils(
        conn=SYNC_LANDSAT_INVENTORY_ID,
        bucket_name=SENTINEL_2_INVENTORY_BUCKET,
        region=AWS_DEFAULT_REGION,
    )
    dest_keys = retrieve_keys_from_inventory_file(s3_bucket_client=s3_inventory_dest)

    orphaned_keys = source_keys - dest_keys
    missing_keys = dest_keys - source_keys

    logging.info(f"orphaned_keys {orphaned_keys}")
    logging.info(f"missing_keys {missing_keys}")

    missing_scenes = [f"s3://sentinel-cogs/{key}" for key in missing_keys]

    output_filename = datetime.today().isoformat() + ".txt"
    key = REPORTING_PREFIX + output_filename

    logging.info(f"output_filename {output_filename}")
    logging.info(f"key {key}")

    # s3_report = S3(conn_id=SYNC_LANDSAT_INVENTORY_ID)
    # s3_report.put_object(
    #     bucket_name=SENTINEL_2_INVENTORY_UTILS_BUCKET,
    #     region="af-south-1",
    #     key=key,
    #     body="\n".join(missing_scenes)
    # )
    #
    # if len(orphaned_keys) > 0:
    #     output_filename = datetime.today().isoformat() + "_orphaned.txt"
    #     key = REPORTING_PREFIX + output_filename
    #     s3_report.put_object(
    #         bucket_name=s3_report.bucket,
    #         key=key,
    #         body="\n".join(orphaned_keys)
    #     )
    #     logging.info(f"Wrote orphaned scenes to: {REPORTING_BUCKET}/{key}")
    #
    # message = (
    #     f"{len(missing_scenes)} scenes are missing from {REPORTING_BUCKET} and {len(orphaned_keys)} "
    #     f"scenes no longer exist in s3://sentinel-cogs"
    # )
    # logging.info(message)
    # if len(missing_scenes) > 200 or len(orphaned_keys) > 200:
    #     raise AirflowException(message)

    # -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
    # s3_inventory = s3(DEST_BUCKET_NAME, AFRICA_CONN_ID, "af-south-1", MANIFEST_SUFFIX)
    # print(f"Processing keys from the inventory file: {s3_inventory.url}")
    #
    # orphaned_keys = set()  # keys that have been removed from sentinel-cogs
    # for bucket, key, *rest in s3_inventory.list_keys():
    #     if ".json" in key and key.startswith(cogs_folder_name):
    #         if key in source_keys:
    #             source_keys.remove(key)
    #         else:
    #             orphaned_keys.add(key)
    #
    # missing_scenes = [f"s3://sentinel-cogs/{key}" for key in source_keys]
    #
    # output_filename = datetime.today().isoformat() + ".txt"
    # key = REPORTING_PREFIX + output_filename
    #
    # s3_report = s3(REPORTING_BUCKET, AFRICA_CONN_ID, "af-south-1")
    # s3_report.s3.put_object(
    #     Bucket=s3_report.bucket, Key=key, Body="\n".join(missing_scenes)
    # )
    # print(f"Wrote inventory to: {REPORTING_BUCKET}/{key}")
    #
    # if len(orphaned_keys) > 0:
    #     output_filename = datetime.today().isoformat() + "_orphaned.txt"
    #     key = REPORTING_PREFIX + output_filename
    #     s3_report.s3.put_object(
    #         Bucket=s3_report.bucket, Key=key, Body="\n".join(orphaned_keys)
    #     )
    #     print(f"Wrote orphaned scenes to: {REPORTING_BUCKET}/{key}")
    #
    # message = f"{len(missing_scenes)} scenes are missing from {REPORTING_BUCKET} and \
    #             {len(orphaned_keys)} scenes no longer exist in s3://sentinel-cogs"
    # print(message)
    #
    # if len(missing_scenes) > 200 or len(orphaned_keys) > 200:
    #     raise AirflowException(message)


with DAG(
    "landsat_scenes_gap_report",
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
    tags=["Landsat_scenes", "status", "gap_report"],
    catchup=False,
) as dag:

    READ_INVENTORIES = PythonOperator(
        task_id="compare_s3_inventories", python_callable=generate_buckets_diff
    )
