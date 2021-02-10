## Utility
Pre-defined workflow dags to support ad-hoc need.

### Annual workflow
Some well established product have annual dataset releases and they need to be added to the database, ows and explorer.

### ows_update
To update ows when needed.

### explorer update
To update explore when needed

## db dump to s3
Problem statement: developers need database dump for local environment for their work.
Requirement: setup a mechanism to automate database dumping and storing for developer access.

regularly pg_dump database and store to s3 bucket.
