"""
# odc database in RDS backup and store to s3

DAG to periodically backup ODC database data.

This DAG uses k8s executors and in cluster with relevant tooling
and configuration installed.
"""
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from textwrap import dedent

from airflow.models import Variable

DB_DUMP_S3_ROLE = Variable.get("db_dump_s3_role", "deafrica-prod-af-eks-db-dump-to-s3")
SECRET_DBA_ADMIN_NAME = Variable.get("db_dba_admin_secret", "dba-admin")
DB_DUMP_S3_BUCKET = Variable.get("db_dump_s3_bucketname", "deafrica-dev-odc-db-dump")


NODE_AFFINITY = {
    "nodeAffinity": {
        "requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [
                {
                    "matchExpressions": [
                        {
                            "key": "nodetype",
                            "operator": "In",
                            "values": [
                                "ondemand",
                            ],
                        }
                    ]
                }
            ]
        }
    }
}

DAG_NAME = "odc_db_dump_to_s3"

INDEXER_IMAGE = "opendatacube/datacube-index:0.0.15"


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
        "DB_HOSTNAME": "db-writer",
        "DB_DATABASE": "odc",
        "AWS_DEFAULT_REGION": "af-south-1",
    },
    # Lift secrets into environment variables
    "secrets": [
        Secret("env", "DB_USERNAME", SECRET_DBA_ADMIN_NAME, "postgres-username"),
        Secret("env", "PGPASSWORD", SECRET_DBA_ADMIN_NAME, "postgres-password"),
    ],
}

# TEST_COMMAND = [
#     "bash",
#     "-c",
#     dedent(
#         """
#             psql -h $(DB_HOSTNAME) -U $(DB_USERNAME) -d $(DB_DATABASE) -t -A -F"," -c "select count(*) from cubedash.product;" > output.csv
#             cat output.csv
#             aws s3 cp output.csv s3://%s/deafrica-prod-af/
#         """
#     )
#     % (DB_DUMP_S3_BUCKET),
# ]


DUMP_TO_S3_COMMAND = [
    "bash",
    "-c",
    dedent(
        """
            pg_dump -h $(DB_HOSTNAME) -U $(DB_USERNAME) -d $(DB_DATABASE) > {0}
            ls -la | grep {0}
            aws s3 cp {0} s3://{1}/deafrica-prod-af
        """
    ).format(f"odc_{date.today().strftime('%Y_%m_%d')}.sql", DB_DUMP_S3_BUCKET),
]

# THE DAG
dag = DAG(
    dag_id=DAG_NAME,
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    schedule_interval="@weekly",  # weekly
    catchup=False,
    tags=["k8s", "developer_support", "rds", "s3", "db"],
)

with dag:
    DB_DUMP = KubernetesPodOperator(
        namespace="processing",
        image=INDEXER_IMAGE,
        arguments=DUMP_TO_S3_COMMAND,
        annotations={"iam.amazonaws.com/role": DB_DUMP_S3_ROLE},
        labels={"step": "ds-arch"},
        name="dump-odc-db",
        task_id="dump-odc-db",
        get_logs=True,
        affinity=NODE_AFFINITY,
        is_delete_operator_pod=True,
    )
    # DB_DUMP_TEST = KubernetesPodOperator(
    #     namespace="processing",
    #     image=INDEXER_IMAGE,
    #     arguments=TEST_COMMAND,
    #     annotations={"iam.amazonaws.com/role": DB_DUMP_S3_ROLE},
    #     labels={"step": "ds-arch"},
    #     name="dump-odc-db",
    #     task_id="dump-odc-db",
    #     get_logs=True,
    #     affinity=NODE_AFFINITY,
    #     is_delete_operator_pod=True,
    # )