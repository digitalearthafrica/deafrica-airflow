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
        "DB_DATABASE": "africa",
        "WMS_CONFIG_PATH": "/env/config/ows_cfg.py",
        "DATACUBE_OWS_CFG": "config.ows_cfg.ows_cfg",
    },
    # Lift secrets into environment variables
    "secrets": [
        Secret("env", "DB_USERNAME", "ows-db", "postgres-username"),
        Secret("env", "DB_PASSWORD", "ows-db", "postgres-password"),
        Secret(
            "env", "AWS_DEFAULT_REGION", "indexing-aws-creds-prod", "AWS_DEFAULT_REGION"
        ),
        Secret(
            "env", "AWS_ACCESS_KEY_ID", "indexing-aws-creds-prod", "AWS_ACCESS_KEY_ID"
        ),
        Secret(
            "env",
            "AWS_SECRET_ACCESS_KEY",
            "indexing-aws-creds-prod",
            "AWS_SECRET_ACCESS_KEY",
        ),
    ],
}

EXPLORER_SECRETS = [
    Secret("env", "DB_USERNAME", "explorer-db", "postgres-username"),
    Secret("env", "DB_PASSWORD", "explorer-db", "postgres-password"),
]

INDEXER_IMAGE = "opendatacube/datacube-index:latest"
OWS_IMAGE = "opendatacube/ows:1.8.3"
EXPLORER_IMAGE = "opendatacube/explorer:2.4"

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
        """
        mkdir -p /env/config;
        curl -s https://raw.githubusercontent.com/digitalearthafrica/config/0.1.1/services/ows_cfg.py --output /env/config/ows_cfg.py;
        datacube-ows-update --views;
        datacube-ows-update s2_l2a;
    """
    ),
]

# ARCHIVE_BASH_COMMAND = [
#     "bash",
#     "-c",
#     dedent(
#         """
#         datacube dataset search -f csv 'product=s2_l2a lon in [170,200]' > /tmp/to_kill.csv;
#         cat /tmp/to_kill.csv | awk -F',' '{print $1}' | sed '1d' > /tmp/to_kill.list;
#         wc -l /tmp/to_kill.list;
#         cat /tmp/to_kill.list | xargs datacube dataset archive
#     """
#     ),
# ]

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
            "deafrica-prod-eks-sentinel-2-indexing",
            "s2_l2a",
        ],
        labels={"step": "sqs-to-rds"},
        name="datacube-index",
        task_id="indexing-task",
        get_logs=True,
        affinity=affinity,
        is_delete_operator_pod=True,
    )

    # ARCHIVE_EXTRANEOUS_DS = KubernetesPodOperator(
    #     namespace="processing",
    #     image=INDEXER_IMAGE,
    #     arguments=ARCHIVE_BASH_COMMAND,
    #     labels={"step": "ds-arch"},
    #     name="datacube-dataset-archive",
    #     task_id="archive-antimeridian-datasets",
    #     get_logs=True,
    #     is_delete_operator_pod=True,
    # )

    OWS_UPDATE_EXTENTS = KubernetesPodOperator(
        namespace="processing",
        image=OWS_IMAGE,
        arguments=OWS_BASH_COMMAND,
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
            "s2_l2a",
        ],
        secrets=EXPLORER_SECRETS,
        labels={"step": "explorer"},
        name="explorer-summary",
        task_id="explorer-summary-task",
        get_logs=True,
        affinity=affinity,
        is_delete_operator_pod=True,
    )

    INDEXING >> OWS_UPDATE_EXTENTS
    INDEXING >> EXPLORER_SUMMARY
