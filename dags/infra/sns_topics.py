"""
# Variables provided from infra to dags
# audit: 11/05/2021
"""
from airflow.models import Variable

# #################### LANDSAT ####################
LANDSAT_SYNC_SNS_ARN = Variable.get(
    "landsat_sync_sns_arn",
    "arn:aws:sns:af-south-1:717690029437:deafrica-dev-eks-landsat-topic",
)  # qa

# #################### SENTINEL - 2  ####################
SENTINEL_2_SYNC_SNS_ARN = Variable.get(
    "sentinel_2_sync_sns_arn",
    "arn:aws:sns:af-south-1:717690029437:sentinel-2-dev-sync-topic",
)  # qa

# #################### SENTINEL - 1  ####################
SENTINEL_1_SYNC_SNS_ARN = Variable.get("sentinel_1_sync_sns_arn", "")  # qa
