"""
## Utility Tool
### ows update ranges
This is utility is to provide administrators the easy accessiblity to run ad-hoc --views and update-ranges

#### default run
    `datacube-ows-update --views`
    `datacube-ows-update s2_nrt_granule_nbar_t`

#### Utility customisation
The DAG can be parameterized with run time configuration `products`

To run with all, set `dag_run.conf.products` to `--all`
otherwise provide products to be refreshed seperated by space, i.e. `s2a_nrt_granule s2b_nrt_granule`
dag_run.conf format:

#### example conf in json format
    "products": "--all"
    "products": "s2a_nrt_granule s2b_nrt_granule"
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from subdags.subdag_ows_views import ows_update_extent_subdag

from infra.variables import (
    DB_DATABASE,
    DB_WRITER,
    REGION,
)

UPDATE_EXTENT_PRODUCTS = []

DAG_NAME = "utility_ows-update-extent"

# DAG CONFIGURATION
DEFAULT_ARGS = {
    "owner": "RODRIGO",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 14),
    "email": ["systems@digitalearthafrica.org"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "env_vars": {
        "DB_HOSTNAME": DB_WRITER,
        "DB_DATABASE": DB_DATABASE,
        "AWS_DEFAULT_REGION": REGION,
    }
}


# THE DAG
dag = DAG(
    dag_id=DAG_NAME,
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=["k8s", "ows"],
)


def parse_dagrun_conf(products, **kwargs):
    """parse para"""
    if products:
        return products
    else:
        return " ".join(UPDATE_EXTENT_PRODUCTS)


SET_REFRESH_PRODUCT_TASK_NAME = "parse_dagrun_conf"

with dag:

    SET_PRODUCTS = PythonOperator(
        task_id=SET_REFRESH_PRODUCT_TASK_NAME,
        python_callable=parse_dagrun_conf,
        op_args=["{{ dag_run.conf.products }}"],
        # provide_context=True,
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

    SET_PRODUCTS >> OWS_UPDATE_EXTENTS
