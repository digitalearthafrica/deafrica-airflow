"""
# S3 buckets provided from infra to dags
# audit: 11/05/2021
"""
from airflow.models import Variable

# #################### LANDSAT ####################
LANDSAT_SYNC_BUCKET_NAME = Variable.get(
    "landsat_sync_bucket_name", "deafrica-landsat-dev"
)  # qa

LANDSAT_INVENTORY_BUCKET_NAME = Variable.get(
    "landsat_inventory_bucket_name", "deafrica-landsat-inventory"
)  # qa

# #################### SENTINEL - 2  ####################
SENTINEL_2_INVENTORY_BUCKET_NAME = Variable.get(
    "sentinel_2_inventory_bucket_name", "deafrica-sentinel-2-inventory"
)  # qa

SENTINEL_2_SYNC_BUCKET_NAME = Variable.get(
    "sentinel_2_sync_bucket_name", "deafrica-sentinel-2-dev"
)  # qa

# #################### SENTINEL - 1  ####################
SENTINEL_1_SYNC_BUCKET_NAME = Variable.get("sentinel_1_sync_bucket_name", "")  # qa

SENTINEL_1_INVENTORY_BUCKET_NAME = Variable.get(
    "sentinel_1_inventory_bucket_name", ""
)  # qa

# #################### DEAFRICA ####################
DEAFRICA_SERVICES_BUCKET_NAME = Variable.get(
    "deafrica_services_bucket_name", "deafrica-services"
)  # qa


# #################### INFRA DB ####################
DB_DUMP_S3_BUCKET = Variable.get("db_dump_s3_bucketname", "deafrica-odc-db-dump")  # qa
