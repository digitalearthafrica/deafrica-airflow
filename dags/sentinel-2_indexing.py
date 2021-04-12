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
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import PythonOperator

from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from subdags.subdag_ows_views import ows_update_extent_subdag
from subdags.subdag_explorer_summary import explorer_refresh_stats_subdag
from infra.podconfig import (
    ONDEMAND_NODE_AFFINITY,
)
from infra.variables import (
    DB_DATABASE,
    DB_HOSTNAME,
    SECRET_ODC_WRITER_NAME,
    SECRET_AWS_NAME,
)
from infra.images import INDEXER_IMAGE, OWS_IMAGE, EXPLORER_IMAGE

from textwrap import dedent

import kubernetes.client.models as k8s

DAG_NAME = "sentinel-2_indexing"

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
        "DB_HOSTNAME": DB_HOSTNAME,
        "DB_DATABASE": DB_DATABASE,
        "DB_PORT": "5432",
    },
    # Lift secrets into environment variables
    "secrets": [
        Secret("env", "DB_USERNAME", "ows-db", "postgres-username"),
        Secret("env", "DB_PASSWORD", "ows-db", "postgres-password"),
        Secret(
            "env",
            "AWS_DEFAULT_REGION",
            "indexing-aws-creds-prod",
            "AWS_DEFAULT_REGION",
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

affinity = ONDEMAND_NODE_AFFINITY

dag = DAG(
    dag_id=DAG_NAME,
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    schedule_interval="0 */1 * * *",
    catchup=False,
    tags=["k8s", "sentinel-2"],
)


def parse_dagrun_conf(product, **kwargs):
    return product


SET_REFRESH_PRODUCT_TASK_NAME = "parse_dagrun_conf"


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

    SET_PRODUCTS = PythonOperator(
        task_id=SET_REFRESH_PRODUCT_TASK_NAME,
        python_callable=parse_dagrun_conf,
        op_args=["s2_l2a"],
        # provide_context=True,
    )

    EXPLORER_SUMMARY = SubDagOperator(
        task_id="run-cubedash-gen-refresh-stat",
        subdag=explorer_refresh_stats_subdag(
            DAG_NAME,
            "run-cubedash-gen-refresh-stat",
            DEFAULT_ARGS,
            SET_REFRESH_PRODUCT_TASK_NAME,
        ),
    )

    OWS_UPDATE_EXTENTS = SubDagOperator(
        task_id="run-ows-update-ranges",
        subdag=ows_update_extent_subdag(
            DAG_NAME,
            "run-ows-update-ranges",
            DEFAULT_ARGS,
            SET_REFRESH_PRODUCT_TASK_NAME,
        ),
    )

    INDEXING >> SET_PRODUCTS
    SET_PRODUCTS >> EXPLORER_SUMMARY
    SET_PRODUCTS >> OWS_UPDATE_EXTENTS
