"""
    Variables to support on the Landsat process
"""
from infra.s3_buckets import LANDSAT_SYNC_S3_BUCKET_NAME
from infra.variables import AWS_DEFAULT_REGION

# ######### Element-84 ############

SENTINEL_COGS_BUCKET = "sentinel-cogs"

SRC_BUCKET_NAME = f"{SENTINEL_COGS_BUCKET}-inventory"

SENTINEL_2_URL = f"https://{SENTINEL_COGS_BUCKET}.s3.us-west-2.amazonaws.com"


# ######### AFRICA ############

AFRICA_TILES = "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/deafrica-mgrs-tiles.csv.gz"

MANIFEST_SUFFIX = "manifest.json"

REPORTING_PREFIX = "status-report/"
