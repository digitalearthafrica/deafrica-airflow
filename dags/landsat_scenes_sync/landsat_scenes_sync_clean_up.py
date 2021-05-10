"""
# TEST DAG
"""

# [START import_module]
import logging
from datetime import timedelta, datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator

from infra.connections import SYNC_LANDSAT_CONNECTION_ID
from infra.s3_buckets import LANDSAT_SYNC_BUCKET_NAME
from landsat_scenes_sync.variables import AWS_DEFAULT_REGION
from utils.aws_utils import S3

# [END import_module]

# [START default_args]

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

    file_name_suffix = f"_stac.json"

    bucket_content_with_stac = set(
        "/".join(key.split("/")[:-1]) for key in list_keys if file_name_suffix in key
    )
    logging.info(
        f"bucket_content_with_stac first 10 {list(bucket_content_with_stac)[0:10]}"
    )
    bucket_content_without_stac = set(
        "/".join(key.split("/")[:-1])
        for key in list_keys
        if "/".join(key.split("/")[:-1]) not in bucket_content_with_stac
    )
    logging.info(
        f"bucket_content_without_stac first 10 {list(bucket_content_without_stac)[0:10]}"
    )
    return bucket_content_without_stac


def check_key_on_s3(conn_id):
    """
    Check if the key is present in an AWS s3 bucket
    :param conn_id:
    :return:
    """
    try:
        s3 = S3(conn_id=conn_id)
        continuation_token = None
        bucket_content = []
        while True:
            resp = s3.list_objects(
                bucket_name=LANDSAT_SYNC_BUCKET_NAME,
                region=AWS_DEFAULT_REGION,
                prefix="collection02/",
                continuation_token=continuation_token,
            )

            if not resp.get("Contents"):
                return

            bucket_content.extend([obj["Key"] for obj in resp["Contents"]])

            # The S3 API is paginated, returning up to 1000 keys at a time.
            # Pass the continuation token into the next response, until we
            # reach the final page (when this field is missing).
            if resp.get("NextContinuationToken"):
                continuation_token = resp["NextContinuationToken"]
            else:
                break

        logging.info(f"bucket_content size {len(bucket_content)}")
        logging.info(f"First 10 {list(bucket_content)[0:10]}")

        path_list_to_be_deleted = filter_path_with_no_stac(bucket_content)
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
