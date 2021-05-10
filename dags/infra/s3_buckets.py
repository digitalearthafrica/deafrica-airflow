"""
# S3 buckets provided from infra to dags
"""
from airflow.models import Variable

# #################### LANDSAT ####################
LANDSAT_SYNC_BUCKET_NAME = Variable.get(
    "landsat_sync_bucket_name", "deafrica-landsat-dev"
)

LANDSAT_INVENTORY_BUCKET_NAME = Variable.get(
    "landsat_inventory_bucket_name", "deafrica-landsat-inventory"
)

# #################### SENTINEL - 2  ####################
SENTINEL_2_INVENTORY_BUCKET_NAME = Variable.get(
    "sentinel_2_inventory_bucket_name", "deafrica-sentinel-2-inventory"
)

SENTINEL_2_SYNC_BUCKET_NAME = Variable.get(
    "sentinel_2_sync_bucket_name", "deafrica-sentinel-2-dev"
)

# #################### DEAFRICA ####################
DEAFRICA_SERVICES_BUCKET_NAME = Variable.get(
    "deafrica_services_bucket_name", "deafrica-services"
)


# #################### INFRA DB ####################
DB_DUMP_S3_BUCKET = Variable.get("db_dump_s3_bucketname", "deafrica-odc-db-dump")
