"""
# Landsat indexing automation
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import PythonOperator

from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from infra.sqs_queues import LANDSAT_INDEX_SQS_NAME
from subdags.subdag_ows_views import ows_update_extent_subdag
from subdags.subdag_explorer_summary import explorer_refresh_stats_subdag
from infra.podconfig import (
    ONDEMAND_NODE_AFFINITY,
)
from infra.variables import DB_DATABASE, DB_HOSTNAME
from infra.images import INDEXER_IMAGE

DAG_NAME = "landsat_indexing"
PRODUCT_NAMES = "ls8_sr ls8_st ls7_sr ls7_st ls5_sr ls5_st"
INDEXING_USER_CREDS = "landsat-indexing-user-creds"

DEFAULT_ARGS = {
    "owner": "Alex Leith",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 14),
    "email": ["systems@digitalearthafrica.org"],
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
        Secret("env", "DB_USERNAME", "odc-writer", "postgres-username"),
        Secret("env", "DB_PASSWORD", "odc-writer", "postgres-password"),
        Secret(
            "env",
            "AWS_DEFAULT_REGION",
            INDEXING_USER_CREDS,
            "AWS_DEFAULT_REGION",
        ),
        Secret("env", "AWS_ACCESS_KEY_ID", INDEXING_USER_CREDS, "AWS_ACCESS_KEY_ID"),
        Secret(
            "env",
            "AWS_SECRET_ACCESS_KEY",
            INDEXING_USER_CREDS,
            "AWS_SECRET_ACCESS_KEY",
        ),
    ],
}

affinity = ONDEMAND_NODE_AFFINITY

dag = DAG(
    dag_id=DAG_NAME,
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    schedule_interval="0 */3 * * *",
    catchup=False,
    tags=["k8s", "landsat"],
)


def parse_dagrun_conf(product, **kwargs):
    """"""
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
            LANDSAT_INDEX_SQS_NAME,
            PRODUCT_NAMES,
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
        op_args=[PRODUCT_NAMES],
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
