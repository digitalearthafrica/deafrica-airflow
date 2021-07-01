"""
## Utility Tool
### update from S3 to Datacube
This is utility is to provide administrators the easy accessiblity to run s3-to-dc

#### Utility utilization
The DAG must be parameterized with run time configurations `s3_glob` and `products`
In addiction the DAG can receive the parameters `no_sign_request` and , `stac`

To run with all, set `dag_run.conf.products` to `--all`
otherwise provide list of products, to be refreshed, separated by space,
i.e. `s2_l2a ls5_sr ls5_st ls7_sr ls7_st`

dag_run.conf format:

#### example conf in json format
    {
        "s3_glob":"s3://deafrica-sentinel-2-dev/sentinel-s2-l2a-cogs/**/*.json",
        "product_definition_uri":"https://raw.githubusercontent.com/digitalearthafrica/config/master/products/ls5_c2l2.odc-product.yaml",
        "products":"s2_l2a ls5_sr ls5_st ls7_sr ls7_st",
        "no_sign_request":true,
        "stac":true
    }
"""
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.secret import Secret
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from infra.images import INDEXER_IMAGE
from infra.podconfig import (
    ONDEMAND_NODE_AFFINITY,
)
from infra.variables import (
    DB_DATABASE,
    DB_WRITER,
    SECRET_ODC_WRITER_NAME,
    REGION,
    DB_PORT,
)
from subdags.subdag_explorer_summary import explorer_refresh_operator

ADD_PRODUCT_TASK_ID = "add-product-task"

INDEXING_TASK_ID = "batch-indexing-task"

DAG_NAME = "utility_index_from_s3"

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
        "DB_PORT": DB_PORT,
        "AWS_DEFAULT_REGION": REGION,
    },
    # Lift secrets into environment variables
    "secrets": [
        Secret("env", "DB_USERNAME", SECRET_ODC_WRITER_NAME, "postgres-username"),
        Secret("env", "DB_PASSWORD", SECRET_ODC_WRITER_NAME, "postgres-password"),
    ],
}


def parse_dagrun_conf(products, **kwargs):
    """
    parse input
    """
    return products


def check_dagrun_config(product_definition_uri: str, s3_glob: str, **kwargs):
    """
    determine task needed to perform
    """
    logging.info(
        f"check_dagrun_config - "
        f"product_definition_uri:{product_definition_uri} - "
        f"s3_glob:{s3_glob} - "
        f"kwargs:{kwargs}"
    )

    if product_definition_uri and s3_glob:
        return [ADD_PRODUCT_TASK_ID, INDEXING_TASK_ID]
    elif product_definition_uri:
        return ADD_PRODUCT_TASK_ID
    elif s3_glob:
        return INDEXING_TASK_ID
    else:
        raise ValueError('Neither product_definition_uri nor s3_glob was informed!')


SET_REFRESH_PRODUCT_TASK_NAME = "parse_dagrun_conf"
CHECK_DAGRUN_CONFIG = "check_dagrun_config"


# THE DAG
dag = DAG(
    dag_id=DAG_NAME,
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=["k8s", "add-product", "self-service", "index-datasets", "explorer-update"],
)

with dag:
    TASK_PLANNER = BranchPythonOperator(
        task_id=CHECK_DAGRUN_CONFIG,
        python_callable=check_dagrun_config,
        op_args=[
            "{{ dag_run.conf.product_definition_uri }}",
            "{{ dag_run.conf.s3_glob }}",
        ],
    )

    ADD_PRODUCT = KubernetesPodOperator(
        namespace="processing",
        image=INDEXER_IMAGE,
        image_pull_policy="Always",
        labels={"step": "datacube-product-add"},
        cmds=["datacube"],
        arguments=[
            "product",
            "add",
            "{{ dag_run.conf.product_definition_uri }}",
        ],
        name="datacube-add-product",
        task_id=ADD_PRODUCT_TASK_ID,
        get_logs=True,
        affinity=ONDEMAND_NODE_AFFINITY,
        is_delete_operator_pod=True,
    )

    op_args = [
        "{{ dag_run.conf.s3_glob }}",
        "{{ dag_run.conf.products }}",
        "{{ dag_run.conf.no_sign_request }}",
        "{{ dag_run.conf.stac }}",
    ]

    INDEXING = KubernetesPodOperator(
        namespace="processing",
        image=INDEXER_IMAGE,
        image_pull_policy="Always",
        labels={"step": "s3-to-dc"},
        cmds=["bash"],
        arguments=[
            "-c",
            "s3-to-dc {stac} {no_sign_request} {s3_glob} {products}"
            "{% if dag_run.conf.stac %}--stac{% endif %} "
            "{% if dag_run.conf.no_sign_request %}--no-sign-request{% endif %} "
            "{{ dag_run.conf.s3_glob }} "
            "{{ dag_run.conf.products }}",
        ],
        name='INDEXING_TEST',
        task_id=INDEXING_TASK_ID,
        get_logs=True,
        affinity=ONDEMAND_NODE_AFFINITY,
        is_delete_operator_pod=True,
        trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,  # Needed in case add product was skipped
    )

    # Retrieve product name argument to be sent to Explorer refresh process
    SET_PRODUCTS = PythonOperator(
        task_id=SET_REFRESH_PRODUCT_TASK_NAME,
        python_callable=parse_dagrun_conf,
        op_args=["{{ dag_run.conf.products }}"],
    )

    # Execute Explorer Refresh process based on the product name
    EXPLORER_SUMMARY = explorer_refresh_operator(
        xcom_task_id=SET_REFRESH_PRODUCT_TASK_NAME,
    )

    TASK_PLANNER >> [ADD_PRODUCT, INDEXING]
    ADD_PRODUCT >> INDEXING >> SET_PRODUCTS >> EXPLORER_SUMMARY
    INDEXING >> SET_PRODUCTS >> EXPLORER_SUMMARY
