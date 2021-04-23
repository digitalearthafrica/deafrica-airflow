"""
    Variables to support on the Landsat process
"""
from infra.s3_buckets import LANDSAT_SYNC_S3_BUCKET_NAME
from infra.variables import AWS_DEFAULT_REGION

# ######### Element-84 ############
SRC_BUCKET_NAME = "sentinel-cogs-inventory"

SENTINEL_COGS_BUCKET = "sentinel-cogs"

# ######### AFRICA ############

AFRICA_TILES = "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/deafrica-mgrs-tiles.csv.gz"

MANIFEST_SUFFIX = "manifest.json"

REPORTING_PREFIX = "status-report/"
