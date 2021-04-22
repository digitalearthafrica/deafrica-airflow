"""
# Connection provided from infra to dags
"""

INDEX_LANDSAT_CONNECTION_ID = "index_landsat_scenes"

# reverting to non-ssm
SYNC_LANDSAT_CONNECTION_ID = "conn_sync_landsat_scene"

SYNC_LANDSAT_INVENTORY_ID = "conn_sync_landsat_scene_inventory"

MIGRATION_OREGON_CONNECTION_ID = "deafrica_migration_oregon"

# Sentinel-2 US connection, this is based on svc-deafrica-prod-eks-s2-data-transfer
# user which is in deafrica account
# S2_US_CONN_ID = "deafrica-prod-eks-s2-data-transfer"
S2_US_CONN_ID = "svc_deafrica_dev_eks_sentinel_2_sync"

# Sentinel-2 PDS connection, this is based on svc-deafrica-sentinel-2-bucket-write
# S2_AFRICA_CONN_ID = "deafrica-sentinel-2-bucket-write"
S2_AFRICA_CONN_ID = "sync_landsat_scene_inventory"
