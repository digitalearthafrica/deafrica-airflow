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
        "no_sign_request":"True",
        "stac":"True"
    }
"""
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.secret import Secret
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule

from subdags.subdag_explorer_summary import explorer_refresh_operator
from infra.images import INDEXER_IMAGE
from infra.podconfig import (
    ONDEMAND_NODE_AFFINITY,
)
from infra.variables import (
    DB_DATABASE,
    DB_HOSTNAME,
    SECRET_ODC_WRITER_NAME,
    AWS_DEFAULT_REGION,
    DB_PORT,
)

ADD_PRODUCT_TASK_ID = "add-product-task"

LOADING_ARGUMENTS_TASK_ID = "loading_arguments_task"

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
        # TODO: Pass these via templated params in DAG Run
        "DB_HOSTNAME": DB_HOSTNAME,
        "DB_DATABASE": DB_DATABASE,
        "DB_PORT": DB_PORT,
        "AWS_DEFAULT_REGION": AWS_DEFAULT_REGION,
    },
    # Lift secrets into environment variables
    "secrets": [
        Secret("env", "DB_USERNAME", SECRET_ODC_WRITER_NAME, "postgres-username"),
        Secret("env", "DB_PASSWORD", SECRET_ODC_WRITER_NAME, "postgres-password"),
    ],
}

# THE DAG
dag = DAG(
    dag_id=DAG_NAME,
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=["k8s", "add-product", "self-service", "index-datasets", "explorer-update"],
)


def parse_dagrun_conf(products, **kwargs):
    """
    parse input
    """
    return products


def loading_arguments(s3_glob: str, products: str, no_sign_request: str, stac: str, **kwargs) -> dict:
    """
    parse input
    """
    logging.info(
        f"Loading Arguments - "
        f"s3_glob:{s3_glob} - "
        f"products:{products} - "
        f"no_sign_request:{no_sign_request} - "
        f"stac:{stac} - "
        f"kwargs:{kwargs}"
    )

    if not s3_glob:
        raise Exception("Need to specify a s3_glob")

    if not products:
        raise Exception("Need to specify a lost of products")

    if stac.lower() == "false":
        stac = ""
    elif stac.lower() == "true":
        stac = "--stac"
    else:
        raise ValueError(f"stac: expected one of 'true', 'false', found {stac}.")

    if no_sign_request.lower() == "false":
        no_sign_request = ""
    elif no_sign_request.lower() == "true":
        no_sign_request = "--no-sign-request"
    else:
        raise ValueError(f"no_sign_request: expected one of 'true', 'false', found {no_sign_request}.")

    logging.info(
        {
            "s3_glob": s3_glob,
            "products": products,
            "stac": stac,
            "no_sign_request": no_sign_request,
        }
    )
    return {
        "s3_glob": s3_glob,
        "products": products,
        "stac": stac,
        "no_sign_request": no_sign_request,
    }


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
        return [ADD_PRODUCT_TASK_ID, LOADING_ARGUMENTS_TASK_ID]
    elif product_definition_uri:
        return ADD_PRODUCT_TASK_ID
    elif s3_glob:
        return LOADING_ARGUMENTS_TASK_ID
    else:
        raise ValueError('Neither product_definition_uri nor s3_glob was informed!')


SET_REFRESH_PRODUCT_TASK_NAME = "parse_dagrun_conf"
CHECK_DAGRUN_CONFIG = "check_dagrun_config"


def indexing_subdag(parent_dag_name, child_dag_name, args, config_task_name):
    """
     Make us a subdag
    """

    logging.info(f"Indexing subdag - "
                 f"parent_dag_name:{parent_dag_name} - "
                 f"child_dag_name:{child_dag_name} - "
                 f"args:{args} - "
                 f"config_task_name:{config_task_name}"
                 )

    subdag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}", default_args=args, catchup=False
    )

    config = "{{{{ task_instance.xcom_pull(dag_id='{}', task_ids='{}') }}}}".format(
        parent_dag_name, config_task_name
    )

    try:
        config = json.loads(config)
    except json.decoder.JSONDecodeError:
        config = {}

    logging.info(f"Retrieved Config - {config}")

    arguments = [
        config.get("s3_glob"),
        config.get("products"),
    ]

    if config.get("stac"):
        arguments.append(config["stac"])

    if config.get("no_sign_request"):
        arguments.append(config["no_sign_request"])

    with subdag:
        KubernetesPodOperator(
            namespace="processing",
            image=INDEXER_IMAGE,
            image_pull_policy="Always",
            labels={"step": "s3-to-dc"},
            # cmds=["s3-to-dc"],
            # arguments=arguments,
            arguments=[
                's3-to-dc',
                config.get("no_sign_request"),
                config.get("stac"),
                config.get("s3_glob"),
                config.get("products"),
            ],
            name=child_dag_name,
            task_id="indexing_id",
            get_logs=True,
            affinity=ONDEMAND_NODE_AFFINITY,
            is_delete_operator_pod=True,
            trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,  # Needed in case add product was skipped
        )

    return subdag


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

    # Validate and retrieve required arguments
    GET_INDEXING_CONFIG = PythonOperator(
        task_id=LOADING_ARGUMENTS_TASK_ID, python_callable=loading_arguments, op_args=op_args
    )

    # Start Indexing process
    INDEXING = SubDagOperator(
        task_id=INDEXING_TASK_ID,
        subdag=indexing_subdag(
            DAG_NAME,
            INDEXING_TASK_ID,
            DEFAULT_ARGS,
            LOADING_ARGUMENTS_TASK_ID,
        ),
        default_args=DEFAULT_ARGS,
        dag=dag,
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

    TASK_PLANNER >> [ADD_PRODUCT, GET_INDEXING_CONFIG]
    GET_INDEXING_CONFIG >> INDEXING >> SET_PRODUCTS >> EXPLORER_SUMMARY
