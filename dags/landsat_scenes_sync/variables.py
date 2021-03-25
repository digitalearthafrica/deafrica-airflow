from infra.variables import (
    LANDSAT_SYNC_S3_BUCKET_NAME,
    AWS_DEFAULT_REGION,
    AWS_DEFAULT_REGION,
)

# ######### AWS CONFIG ############

# ######### USGS ############

USGS_INDEX_URL = "https://landsatlook.usgs.gov/stac-browser/"
USGS_API_MAIN_URL = "https://landsatlook.usgs.gov/sat-api/"
USGS_DATA_URL = "https://landsatlook.usgs.gov/data/"
USGS_S3_BUCKET_NAME = "usgs-landsat"
USGS_AWS_REGION = "us-west-2"

# ######### AFRICA ############
AFRICA_S3_BUCKET_PATH = f"s3://{LANDSAT_SYNC_S3_BUCKET_NAME}/"
AFRICA_S3_PRODUCT_EXPLORER = f"https://explorer.digitalearth.africa/products/"
AFRICA_S3_ENDPOINT = "s3.af-south-1.amazonaws.com"
AFRICA_S3_BUCKET_URL = (
    f"https://{LANDSAT_SYNC_S3_BUCKET_NAME}.s3.{AWS_DEFAULT_REGION}.amazonaws.com/"
)
