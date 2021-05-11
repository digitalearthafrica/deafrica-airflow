"""
# Variables provided from infra to dags
# audit: 11/05/2021
"""
from airflow.models import Variable


LANDSAT_SYNC_SQS_NAME = Variable.get(
    "landsat_sync_sqs_name", "deafrica-prod-af-eks-sync-landsat-scene"
)  # qa

LANDSAT_INDEX_SQS_NAME = Variable.get(
    "landsat_index_sqs_name", "deafrica-prod-af-eks-index-landsat-scene"
)  # qa

SENTINEL_2_SYNC_SQS_NAME = Variable.get(
    "sentinel_2_sync_sqs_name", "deafrica-prod-af-eks-sentinel-2-sync"
)  # qa

SENTINEL_2_INDEX_SQS_NAME = Variable.get(
    "sentinel_2_index_sqs_name", "deafrica-prod-af-eks-sentinel-2-indexing"
)  # qa

SENTINEL_1_INDEX_SQS_NAME = Variable.get("sentinel_1_index_sqs_name", "")  # qa
