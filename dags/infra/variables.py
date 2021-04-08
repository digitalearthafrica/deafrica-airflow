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

LANDSAT_SYNC_S3_C2_FOLDER_NAME = Variable.get("landsat_c2_folder_name", "collection02")

LANDSAT_SYNC_INVENTORY_BUCKET = Variable.get(
    "deafrica_landsat_inventory", "deafrica-landsat-inventory"
)

SENTINEL_2_S3_COGS_FOLDER_NAME = Variable.get(
    "deafrica_sentinel_2_cogs_folder", "sentinel-s2-l2a-cogs"
)

SENTINEL_2_INVENTORY_BUCKET = Variable.get(
    "deafrica_sentinel_2_inventory", "deafrica-sentinel-2-inventory"
)

SENTINEL_2_INVENTORY_UTILS_BUCKET = Variable.get(
    "deafrica_sentinel_2", "deafrica-sentinel-2"
)

SENTINEL_COGS_BUCKET = Variable.get("sentinel_cogs", "sentinel-cogs")

SENTINEL_COGS_INVENTORY_BUCKET = Variable.get(
    "sentinel_cogs_inventory", "sentinel-cogs-inventory"
)

# DB config
DB_DATABASE = Variable.get("db_database", "odc")
DB_HOSTNAME = Variable.get("db_hostname", "db-writer")

AWS_DEFAULT_REGION = Variable.get("aws_default_region", "af-south-1")
SECRET_AWS_NAME = Variable.get("secret_aws_name", "processing-aws-creds-prod")
DB_DUMP_S3_ROLE = Variable.get("db_dump_s3_role", "deafrica-prod-af-eks-db-dump-to-s3")
SECRET_DBA_ADMIN_NAME = Variable.get("db_dba_admin_secret", "dba-admin")
DB_DUMP_S3_BUCKET = Variable.get("db_dump_s3_bucketname", "deafrica-dev-odc-db-dump")

SECRET_EXPLORER_WRITER_NAME = Variable.get(
    "db_explorer_writer_secret", "explorer-writer"
)
SECRET_OWS_WRITER_NAME = Variable.get("db_ows_writer_secret", "ows-writer")
SECRET_ODC_WRITER_NAME = Variable.get("db_odc_writer_secret", "odc-writer")
