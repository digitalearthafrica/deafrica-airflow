"""
## Utility Tool
### explore refresh stats
This is utility is to provide administrators the easy accessiblity to run ad-hoc --refresh-stats

#### default run
    `cubedash-gen --no-init-database --refresh-stats --force-refresh s2a_nrt_granule`
    `cubedash-gen --no-init-database --refresh-stats --force-refresh s2b_nrt_granule`

#### Utility customisation
The DAG can be parameterized with run time configuration `products`

To run with all, set `dag_run.conf.products` to `--all`
otherwise provide products to be refreshed seperated by space, i.e. `s2a_nrt_granule s2b_nrt_granule`
dag_run.conf format:

#### example conf in json format
    "products": "--all"
    "products": "s2a_nrt_granule s2b_nrt_granule"
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator

from infra.variables import (
    DB_DATABASE,
    DB_WRITER,
    REGION,
)
from subdags.subdag_explorer_summary import (
    explorer_refresh_stats_subdag,
)

INDEXING_PRODUCTS = []

DAG_NAME = "utility_explorer-refresh-stats"

# DAG CONFIGURATION
DEFAULT_ARGS = {
    "owner": "Rodrigo",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 14),
    "email": ["systems@digitalearthafrica.org"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "env_vars": {
        "DB_HOSTNAME": DB_WRITER,
        "DB_DATABASE": DB_DATABASE,
        "AWS_DEFAULT_REGION": REGION,
    },
}


# THE DAG
dag = DAG(
    dag_id=DAG_NAME,
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=["k8s", "explorer"],
)


def parse_dagrun_conf(products, **kwargs):
    if products:
        return products
    else:
        return " ".join(INDEXING_PRODUCTS)


SET_REFRESH_PRODUCT_TASK_NAME = "parse_dagrun_conf"

with dag:

    SET_PRODUCTS = PythonOperator(
        task_id=SET_REFRESH_PRODUCT_TASK_NAME,
        python_callable=parse_dagrun_conf,
        op_args=["{{ dag_run.conf.products }}"],
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

    SET_PRODUCTS >> EXPLORER_SUMMARY
