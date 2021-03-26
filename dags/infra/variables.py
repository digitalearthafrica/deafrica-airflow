"""
# Variables provided from infra to dags
"""
from airflow.models import Variable

SYNC_LANDSAT_CONNECTION_SQS_QUEUE = Variable.get(
    "sync_landsat_scenes_sqs_queue", "deafrica-dev-eks-sync-landsat-scene"
)

INDEX_LANDSAT_CONNECTION_SQS_QUEUE = Variable.get(
    "index_landsat_scenes_sqs_queue", "deafrica-dev-eks-index-landsat-scene"
)

LANDSAT_SYNC_SNS_TOPIC_ARN = Variable.get(
    "landsat_sns_topic",
    "arn:aws:sns:af-south-1:717690029437:deafrica-dev-eks-landsat-topic",
)

LANDSAT_SYNC_S3_BUCKET_NAME = Variable.get(
    "landsat_bucket_name", "deafrica-landsat-dev"
)

# DB config
DB_DATABASE = Variable.get("db_database", "odc")
DB_HOSTNAME = Variable.get("db_hostname", "db-writer")

AWS_DEFAULT_REGION = Variable.get("aws_default_region", "af-south-1")
SECRET_AWS_NAME = Variable.get("secret_aws_name", "indexing-aws-creds-dev")
DB_DUMP_S3_ROLE = Variable.get("db_dump_s3_role", "deafrica-dev-eks-db-dump-to-s3")

SECRET_DBA_ADMIN_NAME = Variable.get("db_dba_admin_secret", "dba-admin")
DB_DUMP_S3_BUCKET = Variable.get("db_dump_s3_bucketname", "deafrica-dev-odc-db-dump")

SECRET_EXPLORER_WRITER_NAME = Variable.get("explorer_secret", "explorer-writer")
SECRET_OWS_WRITER_NAME = Variable.get("ows_secret", "ows-writer")
SECRET_ODC_WRITER_NAME = Variable.get("ows_secret", "odc-writer")
