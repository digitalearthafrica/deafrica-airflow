"""
# Variables provided from infra to dags
"""
from airflow.models import Variable


LANDSAT_SYNC_SQS_NAME = Variable.get(
    "landsat_sync_sqs_name", "deafrica-prod-af-eks-sync-landsat-scene"
)

LANDSAT_INDEX_SQS_NAME = Variable.get(
    "landsat_index_sqs_name", "deafrica-prod-af-eks-index-landsat-scene"
)

SENTINEL_2_SYNC_SQS_NAME = Variable.get(
    "sentinel_2_sync_sqs_name", "deafrica-dev-eks-sentinel-2-sync"
)

SENTINEL_2_INDEX_SQS_NAME = Variable.get(
    "sentinel_2_index_sqs_name", "deafrica-prod-af-eks-sentinel-2-indexing"
)
