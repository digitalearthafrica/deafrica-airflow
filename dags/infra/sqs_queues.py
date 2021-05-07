"""
# Variables provided from infra to dags
"""
from airflow.models import Variable


SYNC_LANDSAT_CONNECTION_SQS_QUEUE = Variable.get(
    "sync_landsat_scenes_sqs_queue", "deafrica-dev-eks-sync-landsat-scene"
)

SYNC_LANDSAT_SQS_QUEUE_URL = Variable.get(
    "sync_landsat_scenes_sqs_queue_url",
    "https://sqs.af-south-1.amazonaws.com/717690029437/deafrica-dev-eks-sync-landsat-scene",
)

INDEX_LANDSAT_CONNECTION_SQS_QUEUE = Variable.get(
    "index_landsat_scenes_sqs_queue", "deafrica-dev-eks-index-landsat-scene"
)

SYNC_SENTINEL_2_CONNECTION_SQS_QUEUE = Variable.get(
    "sync_sentinel_2_sqs_queue", "deafrica-dev-eks-sentinel-2-sync"
)

INDEX_SENTINEL_2_CONNECTION_SQS_QUEUE = Variable.get(
    "index_sentinel_2_scenes_sqs_queue", "deafrica-prod-af-eks-sentinel-2-indexing"
)
