"""
# Variables provided from infra to dags
"""
from airflow.models import Variable


LANDSAT_SYNC_SQS_QUEUE = Variable.get(
    "landsat_sync_scenes_sqs_queue", "deafrica-prod-af-eks-sync-landsat-scene"
)

LANDSAT_SYNC_SQS_QUEUE_URL = Variable.get(
    "landsat_sync_scenes_sqs_queue_url",
    "https://sqs.af-south-1.amazonaws.com/717690029437/deafrica-dev-eks-sync-landsat-scene",
)

LANDSAT_INDEX_SQS_QUEUE = Variable.get(
    "landsat_index_scenes_sqs_queue", "deafrica-prod-af-eks-index-landsat-scene"
)

SENTINEL_2_SYNC_SQS_QUEUE = Variable.get(
    "sentinel_2_sync_sqs_queue", "deafrica-dev-eks-sentinel-2-sync"
)

SENTINEL_2_INDEX_SQS_QUEUE = Variable.get(
    "sentinel_2_index_scenes_sqs_queue", "deafrica-prod-af-eks-sentinel-2-indexing"
)
