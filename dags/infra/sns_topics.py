"""
# Variables provided from infra to dags
# audit: 11/05/2021
"""
from airflow.models import Variable

LANDSAT_SYNC_SNS_ARN = Variable.get(
    "landsat_sync_sns_arn",
    "arn:aws:sns:af-south-1:543785577597:deafrica-landsat-scene-topic",
)  # qa

SENTINEL_2_SYNC_SNS_ARN = Variable.get(
    "sentinel_2_sync_sns_arn",
    "arn:aws:sns:af-south-1:543785577597:deafrica-sentinel-2-scene-topic",
)  # qa

SENTINEL_1_SYNC_SNS_ARN = Variable.get(
    "sentinel_1_sync_sns_arn",
    "",
)  # qa
