from airflow.models import Variable

SYNC_LANDSAT_CONNECTION_SQS_QUEUE = Variable.get(
    "landsat_scenes_sqs_queue", "deafrica-dev-eks-sync-landsat-scene"
)

SYNC_LANDSAT_CONNECTION_SNS_QUEUE = Variable.get(
    "landsat_scenes_sns_queue", "deafrica-dev-eks-sync-landsat-sns-scene"
)
