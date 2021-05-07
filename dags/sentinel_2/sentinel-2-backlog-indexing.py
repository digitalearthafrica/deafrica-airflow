"""
# Sentinel-2 backlog indexing automation

DAG to index Sentinel-2 backlog data.

"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.secret import Secret

from infra.s3_buckets import SENTINEL_2_INVENTORY_UTILS_BUCKET
from infra.variables import (
    DB_HOSTNAME,
    SECRET_ODC_WRITER_NAME,
    SENTINEL_2_S3_COGS_FOLDER_NAME,
)

DEFAULT_ARGS = {
    "owner": "Toktam Ebadi",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 14),
    "email": ["toktam.ebadi@ga.gov.au"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "schedule_interval": "@once",
    "retry_delay": timedelta(minutes=5),
    "env_vars": {
        # TODO: Pass these via templated params in DAG Run
        "DB_HOSTNAME": DB_HOSTNAME,
        "WMS_CONFIG_PATH": "/env/config/ows_cfg.py",
        "DATACUBE_OWS_CFG": "config.ows_cfg.ows_cfg",
    },
    # Lift secrets into environment variables
    "secrets": [
        Secret("env", "DB_USERNAME", SECRET_ODC_WRITER_NAME, "postgres-username"),
        Secret("env", "DB_PASSWORD", SECRET_ODC_WRITER_NAME, "postgres-password"),
        Secret("env", "DB_DATABASE", SECRET_ODC_WRITER_NAME, "database-name"),
        Secret(
            "env",
            "AWS_DEFAULT_REGION",
            "sentinel-2-indexing-user",
            "AWS_DEFAULT_REGION",
        ),
        Secret(
            "env", "AWS_ACCESS_KEY_ID", "sentinel-2-indexing-user", "AWS_ACCESS_KEY_ID"
        ),
        Secret(
            "env",
            "AWS_SECRET_ACCESS_KEY",
            "sentinel-2-indexing-user",
            "AWS_SECRET_ACCESS_KEY",
        ),
    ],
}

INDEXER_IMAGE = "opendatacube/datacube-index:0.0.12"

dag = DAG(
    "Sentinel-2_indexing",
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["k8s", "Sentinel-2-indexing"],
)

with DAG(
    "Sentinel-2-backlog-indexing",
    default_args=DEFAULT_ARGS,
    schedule_interval=DEFAULT_ARGS["schedule_interval"],
    tags=["Sentinel-2", "indexing"],
    catchup=False,
) as dag:

    # This needs to be updated in the future in case more zones have been added
    utm_zones = range(26, 42)
    for utm_zone in utm_zones:
        INDEXING = KubernetesPodOperator(
            namespace="processing",
            image=INDEXER_IMAGE,
            image_pull_policy="Always",
            arguments=[
                "s3-to-dc",
                "--stac",
                "--no-sign-request",
                f"s3://{SENTINEL_2_INVENTORY_UTILS_BUCKET}/{SENTINEL_2_S3_COGS_FOLDER_NAME}/{utm_zone}/**/*.json",
                "s2_l2a",
            ],
            labels={"backlog": "s3-to-dc"},
            name="datacube-index",
            task_id=f"Sentinel-2-backlog-indexing-task-utm-zone-{utm_zone}",
            get_logs=True,
            is_delete_operator_pod=True,
        )
