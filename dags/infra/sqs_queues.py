"""
# Variables provided from infra to dags
# audit: 11/05/2021
"""
from airflow.models import Variable

# #################### LANDSAT ####################
LANDSAT_SYNC_SQS_NAME = Variable.get(
    "landsat_sync_sqs_name", "deafrica-pds-landsat-sync-scene"
)  # qa

LANDSAT_SYNC_USGS_SNS_FILTER_SQS_NAME = Variable.get(
    "landsat_usgs_sns_filter_sqs_name", "deafrica-pds-landsat-usgs-sns-filter"
)  # qa

LANDSAT_INDEX_SQS_NAME = Variable.get(
    "landsat_index_sqs_name", "deafrica-prod-af-eks-index-landsat-scene"
)  # qa

# #################### SENTINEL - 2  ####################
SENTINEL_2_SYNC_SQS_NAME = Variable.get(
    "sentinel_2_sync_sqs_name", "deafrica-pds-sentinel-2-sync-scene"
)  # qa

SENTINEL_2_INDEX_SQS_NAME = Variable.get(
    "sentinel_2_index_sqs_name", "deafrica-prod-af-eks-sentinel-2-indexing"
)  # qa

# #################### SENTINEL - 1  ####################
SENTINEL_1_INDEX_SQS_NAME = Variable.get(
    "sentinel_1_index_sqs_name", "deafrica-prod-af-eks-sentinel-1-indexing"
)  # qa
