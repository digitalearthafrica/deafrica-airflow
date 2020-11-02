"""
# Sentinel-2 indexing automation

DAG to periodically index Sentinel-2 data. Eventually it could
update explorer and ows schemas in RDS after a given Dataset has been
indexed.

This DAG uses k8s executors and in cluster with relevant tooling
and configuration installed.

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
    "owner": "Alex Leith",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 14),
    "email": ["alex.leith@ga.gov.au"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
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

EXPLORER_SECRETS = [
    Secret("env", "DB_USERNAME", "explorer-writer", "postgres-username"),
    Secret("env", "DB_PASSWORD", "explorer-writer", "postgres-password")
]

INDEXER_IMAGE = "opendatacube/datacube-index:0.0.12"
OWS_IMAGE = "opendatacube/ows:1.8.1"
EXPLORER_IMAGE = "opendatacube/explorer:2.2.1"

OWS_BASH_COMMAND = [
    "bash",
    "-c",
    dedent(
        """
        mkdir -p /env/config;
        curl -s https://raw.githubusercontent.com/digitalearthafrica/config/0.1.1/services/ows_cfg.py --output /env/config/ows_cfg.py;
        datacube-ows-update --views;
        datacube-ows-update s2_l2a;
    """
    ),
]

dag = DAG(
    "sentinel-2_indexing",
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    schedule_interval="0 */1 * * *",
    catchup=False,
    tags=["k8s", "sentinel-2"],
)

with dag:
    INDEXING = KubernetesPodOperator(
        namespace="processing",
        image=INDEXER_IMAGE,
        image_pull_policy="Always",
        arguments=[
            "sqs-to-dc",
            "--stac",
            "--region-code-list-uri=https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/deafrica-mgrs-tiles.csv.gz",
            "deafrica-prod-af-eks-sentinel-2-indexing",
            "s2_l2a",
        ],
        labels={"step": "sqs-to-rds"},
        name="datacube-index",
        task_id="indexing-task",
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # OWS_UPDATE_EXTENTS = KubernetesPodOperator(
    #     namespace="processing",
    #     image=OWS_IMAGE,
    #     arguments=OWS_BASH_COMMAND,
    #     labels={"step": "ows-mv"},
    #     name="ows-update-extents",
    #     task_id="ows-update-extents",
    #     get_logs=True,
    #     is_delete_operator_pod=True,
    # )

    EXPLORER_SUMMARY = KubernetesPodOperator(
        namespace="processing",
        image=EXPLORER_IMAGE,
        arguments=[
            "cubedash-gen",
            "--no-init-database",
            "--refresh-stats",
            "--force-refresh",
            "s2_l2a",
        ],
        secrets=EXPLORER_SECRETS,
        labels={"step": "explorer"},
        name="explorer-summary",
        task_id="explorer-summary-task",
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # INDEXING >> OWS_UPDATE_EXTENTS
    INDEXING >> EXPLORER_SUMMARY
