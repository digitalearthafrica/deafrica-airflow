"""
# Explorer cubedash-gen refresh-stats subdag
This subdag can be called by other dags
"""

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.secret import Secret
from infra.images import EXPLORER_IMAGE
from infra.variables import SECRET_EXPLORER_WRITER_NAME
from infra.podconfig import NODE_AFFINITY


EXPLORER_SECRETS = [
    Secret("env", "DB_USERNAME", SECRET_EXPLORER_WRITER_NAME, "postgres-username"),
    Secret("env", "DB_PASSWORD", SECRET_EXPLORER_WRITER_NAME, "postgres-password"),
]

INDEXING_PRODUCTS = []


def explorer_refresh_stats_subdag(
    parent_dag_name: str, child_dag_name: str, args: dict, xcom_task_id: str = None
):
    """[summary]

    Args:
        parent_dag_name (str): [Name of parent dag]
        child_dag_name (str): [Name of this dag for parent dag's reference]
        args (dict): [dag arguments]
        xcom_task_id (str, optional): [If this dag needs to xcom_pull a products value set by a pre-task]. Defaults to None.

    Returns:
        [type]: [subdag for processing]
    """

    if xcom_task_id:
        products = (
            "{{{{ task_instance.xcom_pull(dag_id='{}', task_ids='{}') }}}}".format(
                parent_dag_name, xcom_task_id
            )
        )
    else:
        products = " ".join(INDEXING_PRODUCTS)

    EXPLORER_BASH_COMMAND = [
        "bash",
        "-c",
        f"cubedash-gen --no-init-database --refresh-stats --force-refresh {products}",
    ]

    dag_subdag = DAG(
        dag_id="%s.%s" % (parent_dag_name, child_dag_name),
        default_args=args,
        catchup=False,
    )

    KubernetesPodOperator(
        namespace="processing",
        image=EXPLORER_IMAGE,
        arguments=EXPLORER_BASH_COMMAND,
        secrets=EXPLORER_SECRETS,
        labels={"step": "explorer-refresh-stats"},
        name="explorer-summary",
        task_id="explorer-summary-task",
        get_logs=True,
        affinity=NODE_AFFINITY,
        is_delete_operator_pod=True,
        dag=dag_subdag,
    )

    return dag_subdag
