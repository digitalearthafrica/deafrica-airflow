"""
# Sentinel-1 indexing automation

DAG to periodically index Sentinel-1 data.

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

QUEUE_NAME = "deafrica-dev-eks-sentinel-1-indexing-dev"
PRODUCT_NAME = "s1_rtc"
AWS_USER_K8S = "sentinel-1-indexing-user"

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
        Secret("env", "DB_DATABASE", "odc-writer", "database-name"),
        Secret(
            "env",
            "AWS_DEFAULT_REGION",
            AWS_USER_K8S,
            "AWS_DEFAULT_REGION",
        ),
        Secret(
            "env",
            "AWS_ACCESS_KEY_ID",
            AWS_USER_K8S,
            "AWS_ACCESS_KEY_ID"
        ),
        Secret(
            "env",
            "AWS_SECRET_ACCESS_KEY",
            AWS_USER_K8S,
            "AWS_SECRET_ACCESS_KEY",
        )
    ],
}

OWS_SECRETS = [
    Secret("env", "DB_USERNAME", "ows-writer", "postgres-username"),
    Secret("env", "DB_PASSWORD", "ows-writer", "postgres-password"),
    Secret("env", "DB_DATABASE", "ows-writer", "database-name"),
]

EXPLORER_SECRETS = [
    Secret("env", "DB_USERNAME", "explorer-writer", "postgres-username"),
    Secret("env", "DB_PASSWORD", "explorer-writer", "postgres-password"),
    Secret("env", "DB_DATABASE", "explorer-writer", "database-name"),
]

INDEXER_IMAGE = "opendatacube/datacube-index:0.0.12"
OWS_IMAGE = "opendatacube/ows:1.8.2"
EXPLORER_IMAGE = "opendatacube/explorer:2.2.3"

affinity = {
    "nodeAffinity": {
        "requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [
                {
                    "matchExpressions": [
                        {
                            "key": "nodetype",
                            "operator": "In",
                            "values": [
                                "ondemand",
                            ],
                        }
                    ]
                }
            ]
        }
    }
}

OWS_BASH_COMMAND = [
    "bash",
    "-c",
    dedent(
        f"""
        mkdir -p /env/config;
        curl -s https://raw.githubusercontent.com/digitalearthafrica/config/0.1.5/services/deafrica_prod_af.ows_cfg.py --output /env/config/ows_cfg.py;
        datacube-ows-update --views;
        datacube-ows-update {PRODUCT_NAME};
    """
    ),
]

dag = DAG(
    "Sentinel-1_Indexing",
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    schedule_interval="0 */1 * * *",
    catchup=False,
    tags=["k8s", "Sentinel-1"],
)

with dag:
    # TODO: Add --update-if-exists flag to sqs-to-dc
    INDEXING = KubernetesPodOperator(
        namespace="processing",
        image=INDEXER_IMAGE,
        image_pull_policy="Always",
        arguments=[
            "sqs-to-dc",
            "--stac",
            "--update-if-exists",
            "--allow-unsafe",
            QUEUE_NAME,
            PRODUCT_NAME,
        ],
        labels={"step": "sqs-to-rds"},
        name="datacube-index",
        task_id="indexing-task",
        get_logs=True,
        affinity=affinity,
        is_delete_operator_pod=True,
    )

    OWS_UPDATE_EXTENTS = KubernetesPodOperator(
        namespace="processing",
        image=OWS_IMAGE,
        arguments=OWS_BASH_COMMAND,
        secrets=OWS_SECRETS,
        labels={"step": "ows-mv"},
        name="ows-update-extents",
        task_id="ows-update-extents",
        get_logs=True,
        affinity=affinity,
        is_delete_operator_pod=True,
    )

    EXPLORER_SUMMARY = KubernetesPodOperator(
        namespace="processing",
        image=EXPLORER_IMAGE,
        arguments=[
            "cubedash-gen",
            "--no-init-database",
            "--refresh-stats",
            "--force-refresh",
            PRODUCT_NAME,
        ],
        secrets=EXPLORER_SECRETS,
        labels={"step": "explorer"},
        name="explorer-summary",
        task_id="explorer-summary-task",
        get_logs=True,
        affinity=affinity,
        is_delete_operator_pod=True,
    )

    INDEXING
    # INDEXING >> OWS_UPDATE_EXTENTS
    # INDEXING >> EXPLORER_SUMMARY
