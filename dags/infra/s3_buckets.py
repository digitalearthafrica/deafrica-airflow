"""
# s3 buckets
"""
from airflow.models import Variable


SENTINEL_2_INVENTORY_UTILS_BUCKET = Variable.get(
    "deafrica_sentinel_2", "deafrica-sentinel-2"
)

SENTINEL_COGS_BUCKET = Variable.get("sentinel_cogs", "sentinel-cogs")

SENTINEL_COGS_INVENTORY_BUCKET = Variable.get(
    "sentinel_cogs_inventory", "sentinel-cogs-inventory"
)

SENTINEL_2_INVENTORY_BUCKET = Variable.get(
    "deafrica_sentinel_2_inventory", "deafrica-sentinel-2-inventory"
)


LANDSAT_SYNC_S3_BUCKET_NAME = Variable.get(
    "landsat_bucket_name", "deafrica-landsat-dev"
)

LANDSAT_SYNC_INVENTORY_BUCKET = Variable.get(
    "deafrica_landsat_inventory", "deafrica-landsat-inventory"
)

DB_DUMP_S3_BUCKET = Variable.get("db_dump_s3_bucketname", "deafrica-dev-odc-db-dump")
