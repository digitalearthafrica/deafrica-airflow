# Folder components
## images
images used for `KubernetesPodOperator`

## connections
AWS side provisioned airflow connections strings from terraform. If the connection used by a certain DAG is created via the Web UI, do not put the connection id in this file, as it is up to the developer to maintain that connection.

## Variables
Variables supplied by terraform, can also provide default value if terraform is not supplying that value

## podconfig
`KubernetesPodOperator` configurations recommended by infrastracture maintainers.

## S3 bucket
As s3 bucket is global, any s3 bucket name in here can be shared by dev and prod deployments

## S3 SQS Queue
splitting s3 sqs queue from variables for centralised management.

## Tasks Pool
Pools can be created via UI or Backend, store pool in a seperate file for centralised management.

## Iam Roles
iam roles created for `KubernetesPodOperator`, `KubernetesPodOperator` can use iam roles as a more secure setup compared to connections

## Folder Purpose
`infra` folder as name suggests it contains items relating to the underlying infrastracture the airflow instance is running off on. This folder serves to integrate and align with infrastracture supporting the dags the airflow instance is executing.

## Folder Ownership
The upkeep of all items in this `infra` folder is owned by the team managing airflow deployment (cloud team). In the event an infrastracture item is to be renamed, moved or deleted, the cloud team will ensure the infrastracture item change will not effect any DAGS. Any infrastracture item used but not found in this `infra` folder is not managed and maintained by the cloud team, and it is up to the developer to maintain.
