"""
# Generate a gap report between sentinel-cogs and deafica-sentinel-2 buckets

This DAG runs once a month and creates a gap report in the folowing location:
s3://deafrica-sentinel-2/status-report
"""

from datetime import datetime

import re

import pandas as pd
from airflow.operators.python_operator import PythonOperator
from airflow import DAG, AirflowException

from utils.inventory import s3

MANIFEST_SUFFIX = "manifest.json"
AFRICA_TILES = "data/africa-mgrs-tiles.csv"
# The following connection is based on svc-deafrica-prod-eks-s2-data-transfer
# which is in deafrica account
US_CONN_ID = "deafrica-prod-eks-s2-data-transfer"
# The following connection is based on svc-deafrica-sentinel-2-bucket-write
# user in the PDS account
AFRICA_CONN_ID = "deafrica-sentinel-2-bucket-write"
DEST_BUCKET_NAME = "s3://deafrica-sentinel-2-inventory"
SRC_BUCKET_NAME = "s3://sentinel-cogs-inventory"
REPORTING_BUCKET = "s3://deafrica-sentinel-2"
REPORTING_PREFIX = "status-report/"
SCHEDULE_INTERVAL = "@weekly"

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2020, 7, 24),
    "email": ["toktam.ebadi@ga.gov.au", "alex.leith@ga.gov.au"],
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
    source_keys = set()

    africa_tile_ids = set(
        pd.read_csv(
            "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/deafrica-mgrs-tiles.csv.gz",
            header=None,
        ).values.ravel()
    )

    s3_inventory = s3(
        SRC_BUCKET_NAME, US_CONN_ID, "us-west-2", MANIFEST_SUFFIX
    )
    print(f"Processing keys from the inventory file: {s3_inventory.url}")

    for bucket, key, *rest in s3_inventory.list_keys():
        if (
            ".json" in key
            and key.startswith(cogs_folder_name)
            and key.split("/")[-2].split("_")[1] in africa_tile_ids
            # We need to ensure we're ignoring the old format data
            and re.match(r"sentinel-s2-l2a-cogs/\d{4}/", key) is None
        ):
            source_keys.add(key)

    s3_inventory = s3(
        DEST_BUCKET_NAME, AFRICA_CONN_ID, "af-south-1", MANIFEST_SUFFIX
    )
    print(f"Processing keys from the inventory file: {s3_inventory.url}")

    orphaned_keys = set()  # keys that have been removed from sentinel-cogs
    for bucket, key, *rest in s3_inventory.list_keys():
        if ".json" in key and key.startswith(cogs_folder_name):
            if key in source_keys:
                source_keys.remove(key)
            else:
                orphaned_keys.add(key)

    missing_scenes = [f"s3://sentinel-cogs/{key}" for key in source_keys]

    output_filename = datetime.today().isoformat() + ".txt"
    key = REPORTING_PREFIX + output_filename

    s3_report = s3(REPORTING_BUCKET, AFRICA_CONN_ID, "af-south-1")
    s3_report.s3.put_object(
        Bucket=s3_report.bucket, Key=key, Body="\n".join(missing_scenes)
    )
    print(f"Wrote inventory to: {REPORTING_BUCKET}/{key}")

    if len(orphaned_keys) > 0:
        output_filename = datetime.today().isoformat() + "_orphaned.txt"
        key = REPORTING_PREFIX + output_filename
        s3_report.s3.put_object(
            Bucket=s3_report.bucket, Key=key, Body="\n".join(orphaned_keys)
        )
        print(f"Wrote orphaned scenes to: {REPORTING_BUCKET}/{key}")

    message = f"{len(missing_scenes)} scenes are missing from {REPORTING_BUCKET} and \
                {len(orphaned_keys)} scenes no longer exist in s3://sentinel-cogs"
    print(message)

    if len(missing_scenes) > 200 or len(orphaned_keys) > 200:
        raise AirflowException(message)


with DAG(
    "sentinel-2_gap_detection",
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
    tags=["Sentinel-2", "status"],
    catchup=False,
) as dag:

    READ_INVENTORIES = PythonOperator(
        task_id="compare_s2_inventories", python_callable=generate_buckets_diff
    )
