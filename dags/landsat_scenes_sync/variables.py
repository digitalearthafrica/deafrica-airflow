# ######### AWS CONFIG ############

# ######### USGS ############
from infra.variables import AFRICA_S3_BUCKET_NAME, AFRICA_AWS_REGION

USGS_INDEX_URL = "https://landsatlook.usgs.gov/stac-browser/"
USGS_API_MAIN_URL = "https://landsatlook.usgs.gov/sat-api/"
USGS_DATA_URL = "https://landsatlook.usgs.gov/data/"

# ######### AFRICA ############
AFRICA_S3_BUCKET_PATH = f"s3://{AFRICA_S3_BUCKET_NAME}/"
AFRICA_S3_PRODUCT_EXPLORER = f"https://explorer.digitalearth.africa/products/"
AFRICA_S3_ENDPOINT = "s3.af-south-1.amazonaws.com"
AFRICA_S3_BUCKET_URL = (
    f"https://{AFRICA_S3_BUCKET_NAME}.s3.{AFRICA_AWS_REGION}.amazonaws.com/"
)
