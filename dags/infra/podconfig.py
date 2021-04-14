IMAGE_ECR = "538673716275.dkr.ecr.ap-southeast-2.amazonaws.com"

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
