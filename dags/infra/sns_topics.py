"""
# Variables provided from infra to dags
"""
from airflow.models import Variable

LANDSAT_SYNC_SNS_TOPIC_ARN = Variable.get(
    "landsat_sns_topic",
    "arn:aws:sqs:af-south-1:565417506782:deafrica-prod-af-eks-index-landsat-scene",
)

SENTINEL_2_SYNC_TOPIC_ARN = Variable.get(
    "sync_sentinel_2_topic_arn",
    "arn:aws:sns:af-south-1:543785577597:deafrica-sentinel-2-scene-topic",
)
