"""
# TEST DAG
"""

# [START import_module]
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta, datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# [START default_args]
from infra.connections import SYNC_LANDSAT_CONNECTION_ID, SYNC_LANDSAT_INVENTORY_ID
from infra.s3_buckets import LANDSAT_SYNC_S3_BUCKET_NAME, LANDSAT_SYNC_INVENTORY_BUCKET
from landsat_scenes_sync.variables import (
    AWS_DEFAULT_REGION,
    MANIFEST_SUFFIX,
)
from utils.aws_utils import S3
from utils.inventory import InventoryUtils

# [END import_module]

DEFAULT_ARGS = {
    "owner": "rodrigo.carvalho",
    "email": ["rodrigo.carvalho@ga.gov.au"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
    "depends_on_past": False,
    "start_date": datetime(2021, 3, 29),
    "catchup": False,
    "version": "0.2",
}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    "landsat_scenes_sync_cleanup",
    default_args=DEFAULT_ARGS,
    description="Landsat Clean up process",
    schedule_interval=None,
    tags=["Cleanup"],
)


# [END instantiate_dag]


def filter_path_with_no_stac(list_keys):
    """
    Find folders which the sr_stac.json file isn't present
    :param list_keys:
    :return:
    """

    def filter_path(key):
        """
        Filter path
        :param key:
        :return:
        """
        s3 = S3(conn_id=SYNC_LANDSAT_CONNECTION_ID)
        stac_file_name = f'{key.split("/")[-2]}_stac.json'
        returned = s3.key_not_existent(
            bucket_name=LANDSAT_SYNC_S3_BUCKET_NAME,
            key=f"{key}{stac_file_name}",
        )
        return key if returned else None

    num_of_threads = 50
    with ThreadPoolExecutor(max_workers=num_of_threads) as executor:
        tasks = [
            executor.submit(
                filter_path,
                key,
            )
            for key in list_keys
        ]

        return set(future.result() for future in as_completed(tasks) if future.result())


def check_key_on_s3(conn_id):
    """
    Check if the key is present in an AWS s3 bucket
    :param conn_id:
    :return:
    """
    try:
        # Create connection to the inventory S3 bucket
        s3_inventory_dest = InventoryUtils(
            conn=SYNC_LANDSAT_INVENTORY_ID,
            bucket_name=LANDSAT_SYNC_INVENTORY_BUCKET,
            region=AWS_DEFAULT_REGION,
        )
        list_keys = s3_inventory_dest.retrieve_keys_from_inventory(
            manifest_sufix=MANIFEST_SUFFIX
        )

        cleanned_paths = set(
            path.replace(f'{path.split("/")[-1]}', "") for path in list_keys
        )

        path_list_to_be_deleted = filter_path_with_no_stac(cleanned_paths)
        logging.info(f"There are {len(path_list_to_be_deleted)} to be deleted")
        logging.info(f"{path_list_to_be_deleted}")

    except Exception as e:
        logging.error(e)


with dag:
    PythonOperator(
        task_id="Cleanup",
        python_callable=check_key_on_s3,
        op_kwargs=dict(conn_id=SYNC_LANDSAT_CONNECTION_ID),
    )
