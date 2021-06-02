"""
# Connection provided from infra to dags
# audit: 11/05/2021
"""
# ##### LANDSAT #####
# Reads Landsat sync and Inventory bucket
# Reads SQS landsat sync
# Write SQS landsat sync
CONN_LANDSAT_SYNC = "conn_landsat_sync"  # qa

# Reads USGS, Landsat sync
# Writes Landsat sync
# Writes SNS  landsat_sns_topic
CONN_LANDSAT_WRITE = "conn_landsat_write"  # qa

CONN_LANDSAT_INDEX = "conn_landsat_index"  # qa

# ##### SENTINEL - 2 #####
CONN_SENTINEL_2_SYNC = "conn_sentinel_2_sync"  # qa

CONN_SENTINEL_2_INDEX = "conn_sentinel_2_index"  # qa
