"""
    Variables to support on the Landsat process
"""

# ######### Element-84 ############

# #################### COGS ####################
SENTINEL_COGS_BUCKET = "sentinel-cogs"

SENTINEL_COGS_AWS_REGION = "us-west-2"

SENTINEL_COGS_INVENTORY_BUCKET = f"{SENTINEL_COGS_BUCKET}-inventory"

SENTINEL_2_URL = f"https://{SENTINEL_COGS_BUCKET}.s3.{SENTINEL_COGS_AWS_REGION}.amazonaws.com"

# ######### AFRICA ############
# ######## Sentinel 2 ##########

AFRICA_TILES = "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/deafrica-mgrs-tiles.csv.gz"

MANIFEST_SUFFIX = "manifest.json"

REPORTING_PREFIX = "status-report/"

COGS_FOLDER_NAME = "sentinel-s2-l2a-cogs"