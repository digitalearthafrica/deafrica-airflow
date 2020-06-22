"""
# Sentinel-2 indexing automation

DAG to periodically index Sentinel-2 data. Eventually it could 
update explorer and ows schemas in RDS after a given Dataset has been 
indexed.

This DAG uses k8s executors and in cluster with relevant tooling
and configuration installed.

"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.kubernetes.secret import Secret
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.contrib.operators.kubernetes_pod_operator import \
    KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator

import kubernetes.client.models as k8s

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
        "DB_HOSTNAME": "database-write.local",
        "DB_DATABASE": "africa",
    },
    # Lift secrets into environment variables
    "secrets": [
        Secret("env", "DB_USERNAME", "ows-db", "postgres-username"),
        Secret("env", "DB_PASSWORD", "ows-db", "postgres-password"),
        Secret("env", "AWS_DEFAULT_REGION", "indexing-aws-creds-prod", "AWS_DEFAULT_REGION"),
        Secret("env", "AWS_ACCESS_KEY_ID", "indexing-aws-creds-prod", "AWS_ACCESS_KEY_ID"),
        Secret("env", "AWS_SECRET_ACCESS_KEY", "indexing-aws-creds-prod", "AWS_SECRET_ACCESS_KEY"),
    ],
}

OWS_ENV = {
    "WMS_CONFIG_PATH": "/env/config/ows_cfg.py",
    "DATACUBE_OWS_CFG": "config.ows_cfg.ows_cfg"
}

INDEXER_IMAGE = "opendatacube/datacube-index:0.0.7"
OWS_IMAGE = "opendatacube/ows:1.8.0"
EXPLORER_IMAGE = "opendatacube/dashboard:2.1.9"

volume_config= {
    'persistentVolumeClaim': {
            'claimName': 'ows-config'
        }
}
volume = Volume(name='ows-config', configs=volume_config)

init_container_volume_mounts = [
    k8s.V1VolumeMount(
        mount_path='/env/config',
        name='ows-config',
        sub_path=None,
        read_only=True
    )
]

init_container = k8s.V1Container(
  name="init-container",
  image="geoscienceaustralia/deafrica-config:0.1.1-unstable.305.ge30a170",
  volume_mounts=init_container_volume_mounts,
  command=["bash", "-cx"],
  args=["cp", "-f", "/opt/dea-config/services/ows_cfg.py",  "/env/config/ows_cfg.py"]
)

dag = DAG(
    "sentinel-2_indexing",
    doc_md=__doc__,
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=["k8s"]
)

with dag:
    START = DummyOperator(task_id="sentinel-2_indexing")

    INDEXING = KubernetesPodOperator(
        namespace="processing",
        image=INDEXER_IMAGE,
        cmds=["sqs-to-dc"],
        # annotations={"iam.amazonaws.com/role": "svc-deafrica-prod-processing-prod"},
        arguments=["--stac", "deafrica-prod-eks-sentinel-2-indexing", "s2_l2a"],
        labels={"step": "sqs-to-rds"},
        name="datacube-index",
        task_id="indexing-task",
        get_logs=True,
        is_delete_operator_pod=True,
    )

    OWS_UPDATE_MV = KubernetesPodOperator(
        namespace="processing",
        image=OWS_IMAGE,
        cmds=["datacube-ows-update"],
        arguments=["--views", "--blocking"],
        labels={"step": "ows-mv"},
        env_vars=OWS_ENV,
        name="ows-update-materialised-view",
        task_id="ows-update-mv",
        get_logs=True,
        volumes=[volume],
        init_containers=[init_container],
        is_delete_operator_pod=True,
    )

    OWS_UPDATE_PRODUCT = KubernetesPodOperator(
        namespace="processing",
        image=OWS_IMAGE,
        cmds=["datacube-ows-update"],
        arguments=["s2_l2a"],
        labels={"step": "ows-product"},
        env_vars=OWS_ENV,
        name="ows-update-product",
        task_id="ows-update-product",
        get_logs=True,
        volumes=[volume],
        init_containers=[init_container],
        is_delete_operator_pod=True,
    )

    EXPLORER_SUMMARY = KubernetesPodOperator(
        namespace="processing",
        image=EXPLORER_IMAGE,
        cmds=["cubedash-gen"],
        arguments=[
            "--no-init-database",
            "--refresh-stats",
            "--force-refresh",
            "s2_l2a"
        ],
        labels={"step": "explorer"},
        name="explorer-summary",
        task_id="explorer-summary-task",
        get_logs=True,
        is_delete_operator_pod=True,
    )

    COMPLETE = DummyOperator(task_id="all_done")

    START >> INDEXING
    INDEXING >> OWS_UPDATE_MV >> OWS_UPDATE_PRODUCT
    INDEXING >> EXPLORER_SUMMARY
    OWS_UPDATE_PRODUCT >> COMPLETE
    EXPLORER_SUMMARY >> COMPLETE
