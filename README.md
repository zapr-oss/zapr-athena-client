ZAPR AWS Athena Client
=======================================
ZAPR AWS athena client is a python library to run the presto query on the AWS Athena.

At Zapr we have the largest repository of offline media consumption and we try to answer some of the hardest questions of brands, broadcasters and marketers, on top of this data. To make all this happen we have churn TBs of data in a somewhat interactive manner. AWS Athena comes to rescue to help the team achieve this. We are using this client as a middleware to submit the queries to Athena, as Athena has few shortcomings that we have tried to solve through this client.
Athena lacks in :

	1. Submitting multiple queries at a time.
	2. Insert overwrite is not supported in Athena.
	3. Dropping of table doesn't delete the data, only schema is dropped.
Another benefit that we achieve using this client is that we can integrate Athena easily to all our existing data pipelines built on oozie, airflow. 

# Supported Features

* submit the multiple queries from single file.
* insert overwrite.
* drop table (drop the table and delete the data as well).
* submitting the query by using aws athena workgroup. so we can track the cost of the query.

# Quick Start

## Prerequisite

* boto3
* configparser

## Usage


### Syntax
```
python athena_client.py config_file_location workgroup_name query_file_location  input_macro1 input_macro2 ...
```

### Install dependencies
```
pip install -r requirements.txt
```
### Example - 1

```
python athena_client.py config.ini workgroup_testing_team sample-query-file.sql start_date=2020-09-25 end_date=2020-09-25
```

### Example - 2

```
python athena_client.py s3://sampe-bucket/sample-prefix/project-1/config.ini workgroup_testing_team s3://sampe-bucket/sample-prefix/project-1/sample-query-file.sql start_date=2020-09-25 end_date=2020-09-25
```

### Via PIP

```
pip install zapr-athena-client
zapr-athena-client config.ini workgroup_testing_team sample-query-file.sql start_date=2020-09-25 end_date=2020-09-25
```

### Sample Query 
```
create table sample_db.${table_prefix}_username
WITH (external_location = 's3://sample_db/${table_prefix}_username/',format = 'ORC') as
    select username
    from raw_db.users
    where date between '${start_date}' and '${end_date}';
```

### Disable Insert Overwrite and drop data
This athena client supports insert overwrite table and delete data if you are executing drop table query by default. We can add the following configurations to disable these features.

```
ENABLE_INSERT_OVERWRITE = False
ENABLE_EXTERNAL_TABLE_DROP = False
```    
  
Contact
-------
For any features or bugs, please raise it in issues section

If anything else, get in touch with us at opensource@zapr.in