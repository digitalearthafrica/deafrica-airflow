"""
# Variables provided from infra to dags
"""
from airflow.models import Variable

LANDSAT_SYNC_SNS_ARN = Variable.get(
    "landsat_sync_sns_arn",
    "arn:aws:sqs:af-south-1:565417506782:deafrica-prod-af-eks-index-landsat-scene",
)

SENTINEL_2_SYNC_SNS_ARN = Variable.get(
    "sentinel_2_sync_sns_arn",
    "arn:aws:sns:af-south-1:543785577597:deafrica-sentinel-2-scene-topic",
)
