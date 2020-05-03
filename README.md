# cloud_music_modeling

## Overview

This project is focused on building an ETL pipeline in the AWS cloud for a music streaming app, Sparkify.
The data is stored in two s3 buckets, one with log data in JSON, and one with song metadata in JSON.
The goal to have a data mart with normalized tables for analysis in Redshift.

Using
- AWS s3 buckets
- AWS Redshift

## Database Schema

The data is modeled in a star schema

fact table
- songlplays

dimension tables
- users
- songs
- artists
- time

## ETL Pipeline

Before running the pipeline, a Redshift cluster is running with an ARN (amazon resource name) that has S3 read access.


To run the pipeline call  `create_tables.py` then `etl.py` from the terminal.



```shell
python create_tables.py
```

This script
- connects to the Redshift cluster
- drops any tables if they exists
- creates two staging tables to copy the JSON from the s3
- creates normalized tables in the start schema for analytics
- closes the connection to the cluster

```shell
python etl.py
```

This script
- connects to the Redshift cluster
- copies the data from the s3 buckets to the staging tables in the cluster
- inserts data into normalized tables from staging tables
- closes the connection to the cluster


```shell
sql_queries.py
```
 Holds all of the SQL used for creating tables, and copying and inserting data into the Redshift Cluster.
