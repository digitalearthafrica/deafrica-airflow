"""
# Variables provided from infra to dags
"""
from airflow.models import Variable


SYNC_SENTINEL_2_CONNECTION_TOPIC_ARN = Variable.get(
    "sync_sentinel_2_topic_arn",
    "arn:aws:sns:af-south-1:717690029437:sentinel-2-dev-sync-topic",
)
