"""
# Sentinel-2 backlog indexing automation

DAG to index Sentinel-2 backlog data.

"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator

from textwrap import dedent

import kubernetes.client.models as k8s

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
        "DB_HOSTNAME": "db-writer",
        "WMS_CONFIG_PATH": "/env/config/ows_cfg.py",
        "DATACUBE_OWS_CFG": "config.ows_cfg.ows_cfg",
    },
    # Lift secrets into environment variables
    "secrets": [
        Secret("env", "DB_USERNAME", "odc-writer", "postgres-username"),
        Secret("env", "DB_PASSWORD", "odc-writer", "postgres-password"),
        Secret("env", "AWS_DEFAULT_REGION", "sentinel-2-indexing-user", "AWS_DEFAULT_REGION"),
        Secret("env", "AWS_ACCESS_KEY_ID", "sentinel-2-indexing-user", "AWS_ACCESS_KEY_ID"),
        Secret("env", "AWS_SECRET_ACCESS_KEY", "sentinel-2-indexing-user", "AWS_SECRET_ACCESS_KEY"),
        Secret("env", "DB_DATABASE", "odc-writer", "database-name"),
    ],
}

INDEXER_IMAGE = "opendatacube/datacube-index:latest"

dag = DAG(
    "Sentinel-2_indexing",
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["k8s", "Sentinel-2-indexing"],
)

with DAG('Sentinel-2-backlog-indexing', default_args=DEFAULT_ARGS,
         schedule_interval=DEFAULT_ARGS['schedule_interval'],
         tags=["Sentinel-2", "indexing"], catchup=False) as dag:


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
            f"s3://deafrica-sentinel-2/sentinel-s2-l2a-cogs/{utm_zone}/**/*.json",
            "s2_l2a",
        ],
        labels={"backlog": "s3-to-dc"},
        name="datacube-index",
        task_id="Sentinel-2-backlog-indexing-task",
        get_logs=True,
        is_delete_operator_pod=True,
    )
