"""
## Utility Tool (Self Serve)
For indexing more dataset from s3 to an existing established product and ows layers.

#### Utility customisation
The DAG can be parameterized with run time configuration `product` and `s3_glob`

dag_run.conf format:

#### example conf in json format

    "s3_glob": "s3://dea-public-data/cemp_insar/insar/displacement/alos//**/*.yaml",
    "product": "cemp_insar_alos_displacement"
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import PythonOperator
from airflow.kubernetes.secret import Secret
from subdags.subdag_ows_views import ows_update_extent_subdag
from subdags.subdag_explorer_summary import explorer_refresh_stats_subdag
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from infra.images import INDEXER_IMAGE
from infra.variables import (
    DB_DATABASE,
    DB_HOSTNAME,
    SECRET_ODC_WRITER_NAME,
    SECRET_AWS_NAME,
)
from infra.podconfig import (
    ONDEMAND_NODE_AFFINITY,
)

DAG_NAME = "utility_indexing_annual_ows_explorer_update"

# DAG CONFIGURATION
DEFAULT_ARGS = {
    "owner": "Pin Jin",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 14),
    "email": ["pin.jin@ga.gov.au"],
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
        Secret("env", "AWS_DEFAULT_REGION", SECRET_AWS_NAME, "AWS_DEFAULT_REGION"),
        Secret("env", "AWS_ACCESS_KEY_ID", SECRET_AWS_NAME, "AWS_ACCESS_KEY_ID"),
        Secret(
            "env", "AWS_SECRET_ACCESS_KEY", SECRET_AWS_NAME, "AWS_SECRET_ACCESS_KEY"
        ),
    ],
}


# THE DAG
dag = DAG(
    dag_id=DAG_NAME,
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=["k8s", "annual", "batch-indexing", "self-service"],
)


def parse_dagrun_conf(product, **kwargs):
    return product


SET_REFRESH_PRODUCT_TASK_NAME = "parse_dagrun_conf"

with dag:
    INDEXING = KubernetesPodOperator(
        namespace="processing",
        image=INDEXER_IMAGE,
        image_pull_policy="IfNotPresent",
        labels={"step": "s3-to-rds"},
        cmds=["s3-to-dc"],
        arguments=[
            # "s3://dea-public-data/cemp_insar/insar/displacement/alos//**/*.yaml",
            # "cemp_insar_alos_displacement",
            # Jinja templates for arguments
            "{{ dag_run.conf.s3_glob }}",
            "{{ dag_run.conf.product }}",
        ],
        name="datacube-index",
        task_id="batch-indexing-task",
        get_logs=True,
        affinity=ONDEMAND_NODE_AFFINITY,
        is_delete_operator_pod=True,
    )

    SET_PRODUCTS = PythonOperator(
        task_id=SET_REFRESH_PRODUCT_TASK_NAME,
        python_callable=parse_dagrun_conf,
        op_args=["{{ dag_run.conf.product }}"],
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
