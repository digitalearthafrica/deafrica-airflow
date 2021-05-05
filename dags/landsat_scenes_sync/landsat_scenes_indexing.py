"""
# Landsat indexing automation
DAG to periodically index Landsat 5, 7 and 8 data.
This DAG uses k8s executors and in cluster with relevant tooling
and configuration installed.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.secret import Secret

from infra.images import INDEXER_IMAGE
from infra.sqs_queues import INDEX_LANDSAT_CONNECTION_SQS_QUEUE
from infra.variables import DB_HOSTNAME, DB_DATABASE, SECRET_ODC_WRITER_NAME

DEFAULT_ARGS = {
    "owner": "rodrigo.carvalho",
    "email": ["rodrigo.carvalho@ga.gov.au"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=40),
    "start_date": datetime(2021, 3, 29),
    "depends_on_past": False,
    "catchup": False,
    "version": "0.4",
    "env_vars": {
        "DB_HOSTNAME": DB_HOSTNAME,
        "DB_DATABASE": DB_DATABASE,
        "WMS_CONFIG_PATH": "/env/config/ows_cfg.py",
        "DATACUBE_OWS_CFG": "config.ows_cfg.ows_cfg",
    },
    # Lift secrets into environment variables
    "secrets": [
        Secret("env", "DB_USERNAME", SECRET_ODC_WRITER_NAME, "postgres-username"),
        Secret("env", "DB_PASSWORD", SECRET_ODC_WRITER_NAME, "postgres-password"),
        Secret(
            "env",
            "AWS_DEFAULT_REGION",
            "landsat-indexing-user-creds",
            "AWS_DEFAULT_REGION",
        ),
        Secret(
            "env",
            "AWS_ACCESS_KEY_ID",
            "landsat-indexing-user-creds",
            "AWS_ACCESS_KEY_ID",
        ),
        Secret(
            "env",
            "AWS_SECRET_ACCESS_KEY",
            "landsat-indexing-user-creds",
            "AWS_SECRET_ACCESS_KEY",
        ),
    ],
}

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

dag = DAG(
    "landsat_scenes_indexing",
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    schedule_interval="0 */8 * * *",
    tags=["k8s", "landsat-scenes", "indexing"],
)

with dag:
    INDEXING = KubernetesPodOperator(
        namespace="processing",
        image=INDEXER_IMAGE,
        image_pull_policy="Always",
        arguments=[
            "sqs-to-dc",
            "--stac",
            INDEX_LANDSAT_CONNECTION_SQS_QUEUE,
            "ls5_sr ls5_st ls7_sr ls7_st ls8_sr ls8_st",
            "--update-if-exists",
            "--allow-unsafe",
        ],
        labels={"step": "sqs-to-rds"},
        name="datacube-index",
        task_id="indexing-task",
        get_logs=True,
        affinity=affinity,
        is_delete_operator_pod=True,
    )
