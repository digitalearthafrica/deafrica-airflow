"""
# KubernetesPodOperator shared configurations
"""
IMAGE_ECR = "543785577597.dkr.ecr.af-south-1.amazonaws.com"

ONDEMAND_NODE_AFFINITY = {
    "nodeAffinity": {
        "requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [
                {
                    "matchExpressions": [
                        {
                            "key": "nodetype",
                            "operator": "In",
                            "values": [
                                "ondemand",
                            ],
                        }
                    ]
                }
            ]
        }
    }
}

# OWS pod specific configuration
OWS_CFG_FOLDER_PATH = "/env/config/ows_refactored"
OWS_CFG_MOUNT_PATH = "/env/config"
OWS_CFG_PATH = OWS_CFG_MOUNT_PATH + "/ows_refactored/prod_af_ows_root_cfg.py"
OWS_CFG_IMAGEPATH = "/opt/dea-config/services/ows_refactored"
OWS_PYTHON_PATH = "/env/config"
OWS_DATACUBE_CFG = "ows_refactored.prod_af_ows_root_cfg.ows_cfg"
