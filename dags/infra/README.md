## images
images used for `KubernetesPodOperator`

## connections
AWS side provisioned airflow connections strings from terraform

## Variables
Variables supplied by terraform, can also provide default value if terraform is not supplying that value

## podconfig
`KubernetesPodOperator` configurations

## S3 bucket
As s3 bucket is global, any s3 bucket name in here can be shared by dev and prod deployments

## S3 SQS Queue
splitting s3 sqs queue from variables for centralised management.

## Tasks Pool
Pools can be created via UI or Backend, store pool in a seperate file for centralised management.

## Iam Roles
iam roles created for `KubernetesPodOperator`, `KubernetesPodOperator` can use iam roles as a more secure setup compared to connections
