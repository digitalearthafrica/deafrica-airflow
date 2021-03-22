# ######### AWS CONFIG ############

# ######### USGS ############
USGS_S3_BUCKET_NAME = "usgs-landsat"
USGS_AWS_REGION = "us-west-2"
USGS_INDEX_URL = "https://landsatlook.usgs.gov/stac-browser/"
USGS_API_MAIN_URL = "https://landsatlook.usgs.gov/sat-api/"
USGS_DATA_URL = "https://landsatlook.usgs.gov/data/"

# ######### AFRICA ############
AFRICA_SNS_TOPIC_ARN = (
    "arn:aws:sns:af-south-1:717690029437:deafrica-dev-eks-landsat-topic"
)
AFRICA_AWS_REGION = "af-south-1"
AFRICA_S3_BUCKET_NAME = "deafrica-landsat-dev"
AFRICA_S3_BUCKET_PATH = f"s3://{AFRICA_S3_BUCKET_NAME}/"
AFRICA_S3_PRODUCT_EXPLORER = f"https://explorer.digitalearth.africa/products/"
AFRICA_S3_BUCKET_URL = (
    f"https://{AFRICA_S3_BUCKET_NAME}.s3.{AFRICA_AWS_REGION}.amazonaws.com/"
)
