"""
# Landsat indexing automation
DAG to periodically index Landsat 5, 7 and 8 data.
This DAG uses k8s executors and in cluster with relevant tooling
and configuration installed.
"""
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.secret import Secret
from datetime import datetime, timedelta

from infra.images import INDEXER_IMAGE

DEFAULT_ARGS = {
    "owner": "rodrigo.carvalho",
    "email": ["rodrigo.carvalho@ga.gov.au"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
    "depends_on_past": False,
    "start_date": datetime(2021, 2, 2),
    "catchup": False,
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
        Secret("env", "AWS_DEFAULT_REGION", "landsat-indexing-user-creds", "AWS_DEFAULT_REGION"),
        Secret("env", "AWS_ACCESS_KEY_ID", "landsat-indexing-user-creds", "AWS_ACCESS_KEY_ID"),
        Secret("env", "AWS_SECRET_ACCESS_KEY", "landsat-indexing-user-creds", "AWS_SECRET_ACCESS_KEY"),
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
    "landsat-scenes-indexing",
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    # schedule_interval="0 */1 * * *",
    # catchup=False,
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
            "deafrica-dev-eks-index-landsat-scene",
            "'ls5_c2l2 ls7_c2l2 ls8_c2l2'",
        ],
        labels={"step": "sqs-to-rds"},
        name="datacube-index",
        task_id="indexing-task",
        get_logs=True,
        affinity=affinity,
        is_delete_operator_pod=True,
    )
