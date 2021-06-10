"""
# Alchemist Landsat indexing automation
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
)
from infra.images import INDEXER_IMAGE


DAG_NAME = "alchemist_indexing"

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
        Secret("env", "DB_USERNAME", SECRET_ODC_WRITER_NAME, "postgres-username"),
        Secret("env", "DB_PASSWORD", SECRET_ODC_WRITER_NAME, "postgres-password"),
        Secret(
            "env",
            "AWS_DEFAULT_REGION",
            "alchemist-dev-user-creds",
            "AWS_DEFAULT_REGION",
        ),
        Secret(
            "env", "AWS_ACCESS_KEY_ID", "alchemist-dev-user-creds", "AWS_ACCESS_KEY_ID"
        ),
        Secret(
            "env",
            "AWS_SECRET_ACCESS_KEY",
            "alchemist-dev-user-creds",
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
    tags=["k8s", "alchemist"],
)


def parse_dagrun_conf(product, **kwargs):
    return product


SET_REFRESH_PRODUCT_TASK_NAME = "parse_dagrun_conf"

products = {
    "wofs_ls": "deafrica-dev-eks-alchemist-landsat-indexing-dev-wo",
    "fc_ls": "deafrica-dev-eks-alchemist-landsat-indexing-dev-fc"
}

with dag:
    SET_PRODUCTS = PythonOperator(
        task_id=SET_REFRESH_PRODUCT_TASK_NAME,
        python_callable=parse_dagrun_conf,
        op_args=[" ".join(products.keys())]
    )

    for name, queue in products.items():
        INDEXING = KubernetesPodOperator(
            namespace="processing",
            image=INDEXER_IMAGE,
            image_pull_policy="Always",
            arguments=[
                "sqs-to-dc",
                "--stac",
                "--update-if-exists",
                "--allow-unsafe",
                queue,
                name,
            ],
            labels={"step": "sqs-to-rds"},
            name=f"datacube-index-{name}",
            task_id=f"indexing-task-{name}",
            get_logs=True,
            affinity=affinity,
            is_delete_operator_pod=True,
        )

        INDEXING >> SET_PRODUCTS

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

    SET_PRODUCTS >> EXPLORER_SUMMARY
    SET_PRODUCTS >> OWS_UPDATE_EXTENTS
