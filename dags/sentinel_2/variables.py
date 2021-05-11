"""
    Variables to support on the Landsat process
"""

# ######### Element-84 ############

# #################### COGS ####################
SENTINEL_COGS_BUCKET = "sentinel-cogs"

SENTINEL_COGS_INVENTORY_BUCKET = f"{SENTINEL_COGS_BUCKET}-inventory"

SENTINEL_2_URL = f"https://{SENTINEL_COGS_BUCKET}.s3.us-west-2.amazonaws.com"


# ######### AFRICA ############

AFRICA_TILES = "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/deafrica-mgrs-tiles.csv.gz"

MANIFEST_SUFFIX = "manifest.json"

REPORTING_PREFIX = "status-report/"
