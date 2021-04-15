"""
# TEST DAG
"""

# [START import_module]
import logging
from datetime import timedelta, datetime

# The DAG object; we'll need this to instantiate a DAG
import boto3
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook

# Operators; we need this to operate!
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# [END import_module]


# [START default_args]
from infra.connections import SYNC_LANDSAT_CONNECTION_ID
from infra.s3_buckets import LANDSAT_SYNC_S3_BUCKET_NAME
from landsat_scenes_sync.variables import (
    AWS_DEFAULT_REGION,
    USGS_S3_BUCKET_NAME,
    USGS_AWS_REGION,
)
from utils.aws_utils import S3

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
    "landsat_scenes_sync_tests",
    default_args=DEFAULT_ARGS,
    description="Sync Tests",
    schedule_interval=None,
    tags=["TESTs"],
)


# [END instantiate_dag]


def copy_s3_to_s3_boto3(
    conn_id: str,
    source_bucket: str,
    destination_bucket: str,
    source_bucket_region: str,
    destination_bucket_region: str,
    source_key: str,
    destination_key: str = None,
    request_payer: str = "requester",
    acl: str = "bucket-owner-full-control",
):
    """
    Function to copy files from one S3 source_bucket_client to another.

    :param destination_bucket_region:
    :param source_bucket_region:
    :param source_key:(str) Source file path
    :param destination_key:(str) Destination file path
    :param conn_id:(str) Airflow connection id
    :param source_bucket:(str) Source S3 source_bucket_client name
    :param destination_bucket:(str) Destination S3 source_bucket_client name
    :param request_payer:(str) When None the S3 owner will pay, when <requester> the solicitor will pay
    :param acl:

    :return: None
    """

    if source_key and not destination_key:
        # If destination_key is not informed, build the same structure as the source_key
        destination_key = source_key

    logging.info(f"copy_s3_to_s3 source: {source_key} destination: {destination_key}")

    aws_hook = AwsHook(aws_conn_id=conn_id)
    cred = aws_hook.get_session().get_credentials()

    source_bucket_client = boto3.client(
        "s3",
        aws_access_key_id=cred.access_key,
        aws_secret_access_key=cred.secret_key,
        region_name=source_bucket_region,
    )

    s3_obj = source_bucket_client.get_object(
        Bucket=source_bucket, Key=source_key, RequestPayer=request_payer
    )

    if hasattr(s3_obj, "Body"):
        logging.info(f"hasattr(s3_obj, body) {hasattr(s3_obj, 'Body')}")
    else:
        logging.info(f"ELSE {s3_obj}")

    streaming_body = s3_obj["Body"]

    logging.info(f"streaming_body {streaming_body}")

    destination_bucket_client = boto3.client(
        "s3",
        aws_access_key_id=cred.access_key,
        aws_secret_access_key=cred.secret_key,
        region_name=destination_bucket_region,
    )
    returned = destination_bucket_client.upload_fileobj(
        streaming_body, destination_bucket, destination_key, ExtraArgs=dict(ACL=acl)
    )
    logging.info(f"RETURNED {returned}")


def check_key_on_s3(conn_id):
    try:
        bucket = S3Hook(aws_conn_id=conn_id)

        not_exist = bucket.get_conn().head_object(
            Bucket=LANDSAT_SYNC_S3_BUCKET_NAME,
            Key="collection02/level-2/standard/oli-tirs/2015/195/044/LC08_L2SP_195044_20151102_20210219_02_T1/LC08_L2SP_195044_20151102_20210219_02_T1_SR_B5.TIF",
        )
        logging.info(f"The key exist {not_exist}")

        not_exist2 = bucket.get_conn().head_object(
            Bucket=LANDSAT_SYNC_S3_BUCKET_NAME,
            Key="collection02/level-2/standard/oli-tirs/2015/195/044/LC08_L2SP_195044_20151102_20210219_02_T1/LC08_L2SP_195044_20151102_20210219_02_T1_SR_B88.TIF",
        )
        logging.info(f"The key exist {not_exist2}")
    except Exception as e:
        logging.error(e)


with dag:
    START = DummyOperator(task_id="start-tasks")

    path_list = [
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_ANG.txt",
        "collection02/level-2/standard/oli-tirs/2013/191/038/LC08_L2SP_191038_20130329_20200913_02_T1/LC08_L2SP_191038_20130329_20200913_02_T1_ST_B10.TIF",
    ]

    # processes = []
    # count = 0
    # for path in path_list:
    #     processes.append(
    #         PythonOperator(
    #             task_id=f"TEST-{count}",
    #             python_callable=copy_s3_to_s3_boto3,
    #             op_kwargs=dict(
    #                 conn_id=SYNC_LANDSAT_CONNECTION_ID,
    #                 source_bucket=USGS_S3_BUCKET_NAME,
    #                 destination_bucket=LANDSAT_SYNC_S3_BUCKET_NAME,
    #                 source_bucket_region=USGS_AWS_REGION,
    #                 destination_bucket_region=AWS_DEFAULT_REGION,
    #                 source_key=path,
    #                 destination_key=path,
    #                 request_payer="requester",
    #                 acl="bucket-owner-full-control",
    #             ),
    #             dag=dag,
    #         )
    #     )
    #     count += 1

    processes = [
        PythonOperator(
            task_id="TEST-Check_key",
            python_callable=check_key_on_s3,
            op_kwargs=dict(conn_id=SYNC_LANDSAT_CONNECTION_ID),
        )
    ]

    END = DummyOperator(task_id="end-tasks")

    START >> processes >> END
