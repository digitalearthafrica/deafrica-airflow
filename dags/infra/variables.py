from airflow.models import Variable

SYNC_LANDSAT_CONNECTION_SQS_QUEUE = Variable.get(
    "sync_landsat_scenes_sqs_queue", "deafrica-dev-eks-sync-landsat-scene"
)

INDEX_LANDSAT_CONNECTION_SQS_QUEUE = Variable.get(
    "index_landsat_scenes_sqs_queue", "deafrica-dev-eks-index-landsat-scene"
)
