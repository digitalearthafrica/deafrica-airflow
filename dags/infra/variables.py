from airflow.models import Variable

SYNC_LANDSAT_CONNECTION_SQS_QUEUE = Variable.get(
    "landsat_scenes_sqs_queue", "deafrica-dev-eks-sync-landsat-scene"
)
