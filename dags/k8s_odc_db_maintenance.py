"""### AWS ODC datacube db maintenance job

Daily DAG for AWS ODC datacube db maintenance operations
"""

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.secret import Secret
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from textwrap import dedent
from infra.images import INDEXER_IMAGE
from infra.podconfig import ONDEMAND_NODE_AFFINITY
from infra.variables import DB_HOSTNAME, DB_PORT, SECRET_ODC_ADMIN_NAME

# Templated DAG arguments
DEFAULT_ARGS = {
    "owner": "Nikita Gandhi",
    "depends_on_past": False,
    "start_date": datetime(2021, 6, 12),
    "email": ["nikita.gandhi@ga.gov.au"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "env_vars": {
        "DB_HOSTNAME": DB_HOSTNAME,
        "DB_PORT": DB_PORT,
    },
    # Use K8S secrets to send DB Creds
    # Lift secrets into environment variables for datacube database connectivity
    "secrets": [
        Secret("env", "DB_DATABASE", SECRET_ODC_ADMIN_NAME, "database-name"),
        Secret("env", "DB_ADMIN_USER", SECRET_ODC_ADMIN_NAME, "postgres-username"),
        Secret("env", "PGPASSWORD", SECRET_ODC_ADMIN_NAME, "postgres-password"),
    ],
}

dag = DAG(
    "k8s_odc_db_maintenance",
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    catchup=False,
    concurrency=1,
    max_active_runs=1,
    tags=["k8s", "db", "odc"],
    schedule_interval="@weekly",
    dagrun_timeout=timedelta(minutes=60 * 4),
)

affinity = ONDEMAND_NODE_AFFINITY

MAINTENANCE_SCRIPT = [
    "bash",
    "-c",
    dedent(
        """
            # vaccum analyze agdc + cubedash tables
            psql -h $(DB_HOSTNAME) -U $(DB_ADMIN_USER) -d $(DB_DATABASE) -c \
            "vacuum verbose analyze agdc.dataset, agdc.dataset_type, agdc.dataset_source, agdc.metadata_type, agdc.dataset_location, cubedash.dataset_spatial, cubedash.product, cubedash.region, cubedash.time_overview;"
        """
    ),
]


with dag:
    START = DummyOperator(task_id="start")

    ODC_DB_MAINTENANCE = KubernetesPodOperator(
        namespace="processing",
        image=INDEXER_IMAGE,
        arguments=MAINTENANCE_SCRIPT,
        image_pull_policy="Always",
        labels={"step": "odc-db-maintenance"},
        name="odc-db-maintenance",
        task_id="odc-db-maintenance",
        get_logs=True,
        is_delete_operator_pod=True,
        affinity=affinity,
    )

    # Task complete
    COMPLETE = DummyOperator(task_id="done")

    START >> ODC_DB_MAINTENANCE
    ODC_DB_MAINTENANCE >> COMPLETE