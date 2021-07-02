"""
    Variables to support on the Landsat process
"""
from infra.s3_buckets import LANDSAT_SYNC_BUCKET_NAME
from infra.variables import REGION

# ######### USGS ############

USGS_BASE_URL = "https://landsatlook.usgs.gov/"

USGS_API_MAIN_URL = f"{USGS_BASE_URL}sat-api/"

USGS_API_INDIVIDUAL_ITEM_URL = f"{USGS_API_MAIN_URL}collections/landsat-c2l2-sr/items"

USGS_INDEX_URL = f"{USGS_BASE_URL}stac-browser/"

USGS_DATA_URL = f"{USGS_BASE_URL}data/"

USGS_S3_BUCKET_NAME = "usgs-landsat"

USGS_S3_BUCKET_PATH = f"s3://{USGS_S3_BUCKET_NAME}/"

USGS_AWS_REGION = "us-west-2"

BASE_BULK_CSV_URL = "https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/"

# ######### AFRICA ############

AFRICA_S3_BUCKET_PATH = f"s3://{LANDSAT_SYNC_BUCKET_NAME}/"

AFRICA_S3_PRODUCT_EXPLORER = "https://explorer.digitalearth.africa/products/"

AFRICA_S3_ENDPOINT = "s3.af-south-1.amazonaws.com"

AFRICA_S3_BUCKET_URL = f"https://{LANDSAT_SYNC_BUCKET_NAME}.s3.{REGION}.amazonaws.com/"

MAIN_GITHUB_DIGITALAFRICA_URL = "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/"

AFRICA_GZ_TILES_IDS_URL = f"{MAIN_GITHUB_DIGITALAFRICA_URL}deafrica-mgrs-tiles.csv.gz"

AFRICA_GZ_PATHROWS_URL = f"{MAIN_GITHUB_DIGITALAFRICA_URL}deafrica-usgs-pathrows.csv.gz"

MANIFEST_SUFFIX = "manifest.json"

C2_FOLDER_NAME = "collection02"

STATUS_REPORT_FOLDER_NAME = "status-report"