"""
# Sentinel-2 geomedian product indexing automation

DAG to index Sentinel-2 geomedian backlog data.

"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import (
    KubernetesPodOperator,
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
        "DB_HOSTNAME": "db-writer",
        "WMS_CONFIG_PATH": "/env/config/ows_cfg.py",
        "DATACUBE_OWS_CFG": "config.ows_cfg.ows_cfg",
    },
    # Lift secrets into environment variables
    "secrets": [
        Secret("env", "DB_USERNAME", "odc-writer", "postgres-username"),
        Secret("env", "DB_PASSWORD", "odc-writer", "postgres-password"),
        Secret(
            "env",
            "AWS_DEFAULT_REGION",
            "sentinel-2-indexing-user",
            "AWS_DEFAULT_REGION",
        ),
        Secret(
            "env",
            "AWS_ACCESS_KEY_ID",
            "sentinel-2-indexing-user",
            "AWS_ACCESS_KEY_ID",
        ),
        Secret(
            "env",
            "AWS_SECRET_ACCESS_KEY",
            "sentinel-2-indexing-user",
            "AWS_SECRET_ACCESS_KEY",
        ),
        Secret("env", "DB_DATABASE", "odc-writer", "database-name"),
    ],
}

INDEXER_IMAGE = "opendatacube/datacube-index:0.0.12"

dag = DAG(
    "Sentinel-2-geomedian-indexing",
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["k8s", "Sentinel-2-indexing"],
)

with DAG(
    "Sentinel-2-geomedian-backlog-indexing",
    default_args=DEFAULT_ARGS,
    schedule_interval=DEFAULT_ARGS["schedule_interval"],
    tags=["Sentinel-2", "geomedian-indexing"],
    catchup=False,
) as dag:

    # This needs to be updated in the future in case more zones have been added
    x = range(153, 246)
    for index in x:
        INDEXING = KubernetesPodOperator(
            namespace="processing",
            image=INDEXER_IMAGE,
            image_pull_policy="Always",
            arguments=[
                "s3-to-dc",
                "--stac",
                "--no-sign-request",
                f"s3://deafrica-services/gm_s2_annual/1-0-0/x{index}/**/*.json",
                "s2_l2a",
            ],
            labels={"backlog": "s3-to-dc"},
            name="datacube-index",
            task_id=f"Sentinel-2-geomedian-backlog-indexing-task-x{index}",
            get_logs=True,
            is_delete_operator_pod=True,
        )
