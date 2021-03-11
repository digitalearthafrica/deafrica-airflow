"""
# Sentinel-2_nrt process docker images used
"""

# IMAGES USED FOR THIS DAG
# INDEXER_IMAGE = "538673716275.dkr.ecr.ap-southeast-2.amazonaws.com/opendatacube/datacube-index:0.0.15"

# OWS_IMAGE = "538673716275.dkr.ecr.ap-southeast-2.amazonaws.com/opendatacube/ows:1.8.2"
# OWS_CONFIG_IMAGE = "538673716275.dkr.ecr.ap-southeast-2.amazonaws.com/geoscienceaustralia/dea-datakube-config:1.5.5"


# EXPLORER_IMAGE = (
#     "538673716275.dkr.ecr.ap-southeast-2.amazonaws.com/opendatacube/explorer:2.2.4"
# )

INDEXER_IMAGE = "opendatacube/datacube-index:latest"
OWS_IMAGE = "opendatacube/ows:1.8.2"
EXPLORER_IMAGE = "opendatacube/explorer:2.4.0"
OWS_CONFIG_IMAGE = "geoscienceaustralia/deafrica-config:latest"

# UNSTABLE IMAGES
EXPLORER_UNSTABLE_IMAGE = "opendatacube/explorer:2.4.3-65-ge372da5"
