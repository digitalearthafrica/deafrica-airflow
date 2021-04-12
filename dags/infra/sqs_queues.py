"""
# Variables provided from infra to dags
"""
from airflow.models import Variable

SYNC_LANDSAT_CONNECTION_SQS_QUEUE = Variable.get(
    "sync_landsat_scenes_sqs_queue", "deafrica-prod-af-eks-sync-landsat-scene"
)

INDEX_LANDSAT_CONNECTION_SQS_QUEUE = Variable.get(
    "index_landsat_scenes_sqs_queue", "deafrica-prod-af-eks-index-landsat-scene"
)
