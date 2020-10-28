#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import boto3
from ..utils.Utils import Utils
from .Glue import Glue
from .zaprs3client import zaprS3Client
import sys
import os
from botocore.exceptions import ParamValidationError, ClientError
import time
import re

class Athena:
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    QUEUED = "QUEUED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    def __init__(self,
                 logger,
                 region,
                 work_group,
                 athena_result_location,
                 staging_database):
        self.logger = logger
        self.client = boto3.client('athena', region_name=region)
        self.work_group = work_group
        self.athena_result_location = athena_result_location
        self.staging_database = staging_database
        self.utils = Utils(logger)
        self.glue = Glue(logger, region)
        self.zapr_s3_client = zaprS3Client(logger)

    def submit_athena_query(self, query_string):
        try:
            athena_response = self.client.start_query_execution(
                QueryString=query_string,
                ResultConfiguration={
                    'OutputLocation': self.athena_result_location
                },
                WorkGroup=self.work_group)
        except ParamValidationError as e:
            self.logger.fatal("Parameter validation error: %s" % e)
            sys.exit(os.EX_OSERR)
        except ClientError as e:
            self.logger.fatal("Unexpected error: %s" % e)
            sys.exit(os.EX_OSERR)
        self.logger.info("Response from Athena submission {0}".format(athena_response))
        query_id = athena_response['QueryExecutionId']
        self.logger.info("Query id {0}".format(query_id))
        return query_id

    def sync_get_status(self, query_id):
        while True:
            try:
                response = self.client.get_query_execution(
                    QueryExecutionId=query_id)
            except ParamValidationError as e:
                self.logger.fatal("Parameter validation error: %s" % e)
                sys.exit(os.EX_OSERR)

            status = response['QueryExecution']['Status']['State']
            self.logger.info("Current Status for {0} (state: {1})".format(query_id, status))
            if status == self.SUCCEEDED:
                manifest = self.athena_result_location + query_id + "-manifest.csv"
                result_metadata = self.athena_result_location + query_id + "metadata"
                self.logger.debug(
                    "You can download the meta data of the results from {0}, {1}".format(manifest, result_metadata))
                break
            elif status == self.FAILED or status == self.CANCELLED:
                self.logger.error("Query {0}....".format(status))
                self.logger.error("REASON : {0}".format(response['QueryExecution']['Status']['StateChangeReason']))
                sys.exit(os.EX_OSERR)
            else:
                time.sleep(5)
                continue

    def drop_table_query(self, query, enable_external_table_drop):
        if enable_external_table_drop == 'False':
            query_execution_id = self.submit_athena_query(query_string=query)
            self.sync_get_status(query_execution_id)
        else:
            table_name = self.utils.get_table_name_drop_table(query)

            if table_name is None:
                self.logger.error("Error in drop table query {0}".format(query))
                sys.exit(os.EX_IOERR)

            database, table = self.utils.get_database_table(table_name)
            table_data_location = self.glue.get_table_data_location(database, table)
            self.logger.info("Deleting table {0}".format(table_name))
            query_execution_id = self.submit_athena_query(query_string=query)
            self.sync_get_status(query_execution_id)
            if table_data_location is not None:
                self.logger.info("Deleting the data from s3....{0}".format(table_data_location))
                bucket, prefix = self.utils.split_s3_path(table_data_location)
                self.zapr_s3_client.delete_objects(bucket, prefix)

    def create_table_query(self, query):
        query_execution_id = self.submit_athena_query(query_string=query)
        self.sync_get_status(query_execution_id)

    def insert_into_table_query(self, query, enable_insert_overwrite):
        if enable_insert_overwrite  == 'False':
            query_execution_id = self.submit_athena_query(query_string=query)
            self.sync_get_status(query_execution_id)
        else:
            """
            insert into table in athena is appending the data in the existing table. But we need to replace the old data
            with new data. We are solving this via creating a staging table and insert into it and replace the data in
            the destination location
            """
            table_name = self.utils.get_table_name_from_insert_query(insert_into_query=query)
            if table_name is None:
                self.logger.info("Not able to figure out the table from INSERT INTO query")
                sys.exit(os.EX_OSERR)

            data_base, table_name = self.utils.get_database_table(table_name=table_name)
            table_property = self.glue.get_table_property(database=data_base,
                                                          table=table_name)

            if table_property is None:
                self.logger.error("Table is not present in the aws glue")
                sys.exit(os.EX_OSERR)
            self.logger.info("Table Property : {0}".format(table_property))

            """ create a staging table in the staging database with same table property as the given table.
                The changes in the staging table are storage location of the staging table and table name
                 ex : location will be in athena result location and table name will be staging_given_table_current_timestamp
            """
            table_storage_descriptor = table_property['Table']['StorageDescriptor']
            storage_location = table_storage_descriptor['Location']
            if storage_location.endswith('/'):
                storage_location = storage_location[:-1]

            partition_keys = table_property['Table']['PartitionKeys']

            millis = int(round(time.time() * 1000))
            staging_table_name = 'STAGING_' + table_name + '_' + str(millis)
            self.logger.info("STAGING Table name : {0}".format(staging_table_name))

            staging_table_property, staging_table_storage_location = self.utils.set_staging_table_property(
                staging_table_name=staging_table_name,
                athena_result_location=self.athena_result_location,
                table_storage_descriptor=table_storage_descriptor,
                table_partition_keys=partition_keys)
            self.logger.info("Creating staging table with property : {0} and  name : {1} ".format(staging_table_property,
                                                                                                  staging_table_name))
            # todo get repsonse code
            self.glue.create_table_in_glue(self.staging_database, staging_table_property)

            """
            Inserting data into the staging table
            """
            self.logger.info("Inserting into staging table.....")
            query = re.sub(data_base + "." + table_name, self.staging_database + "." + staging_table_name, query,
                           count=1)
            self.logger.info("Insert query for staging table {0} : ".format(query))
            query_execution_id = self.submit_athena_query(query_string=query)
            self.sync_get_status(query_execution_id)

            if partition_keys is None or len(partition_keys)==0:
                """If the given table does not have partition 
                then we do not need to get the resultant partitions from staging table"""
                """Move the data from staging table storage location to final table storage location"""
                staging_table_storage_bucket, staging_table_storage_s3_prefix = self.utils.split_s3_path(
                    staging_table_storage_location)
                final_table_storage_bucket, final_table_storage_s3_prefix = self.utils.split_s3_path(storage_location)

                self.zapr_s3_client.move_objects(source_bucket=staging_table_storage_bucket,
                                                 source_prefix=staging_table_storage_s3_prefix,
                                                 destination_bucket=final_table_storage_bucket,
                                                 destination_prefix=final_table_storage_s3_prefix)

                self.logger.debug("Dropping the staging table : {0}.{1}".format(self.staging_database, staging_table_name))

                drop_table_query = "drop table {0}.{1}".format(self.staging_database, staging_table_name)
                query_execution_id = self.submit_athena_query(drop_table_query)
                self.sync_get_status(query_execution_id)


            else:
                """ get the partitions from the staging table. 
                so we can replace those partition's data in the given original table"""
                staging_table_partition_response = self.glue.get_partitions(database=self.staging_database,
                                                                            table=staging_table_name)
                staging_table_partitions = staging_table_partition_response['Partitions']

                """ If there are no partitions in the staging table response, then there are 2 possibilities.
                result of insert into query did not generate any data(empty result) or 
                the given table itself is un partitioned table"""

                final_table_storage_bucket, final_table_storage_s3_prefix = self.utils.split_s3_path(storage_location)
                if staging_table_partitions:
                    length = len(staging_table_partitions)
                    for i in range(length):
                        ''''sample: table partition s3://source-bucket/s3prefix/date=1900-01-01'''
                        ''''sample: table partition s3://source-bucket/s3prefix/date=1900-01-02'''
                        table_partition = staging_table_partitions[i]['StorageDescriptor']['Location']

                        partition = table_partition.replace(staging_table_storage_location, "")

                        '''staging_table_storage_bucket : s3://source-bucket/ 
                           staging_table_storage_s3_prefix : s3prefix/date=1900-01-01
                        '''
                        staging_table_storage_bucket, staging_table_storage_s3_prefix = self.utils.split_s3_path(
                            table_partition)

                        destination_table_storage_s3_prefix = final_table_storage_s3_prefix + partition
                        self.zapr_s3_client.move_objects(source_bucket=staging_table_storage_bucket,
                                                         source_prefix=staging_table_storage_s3_prefix,
                                                         destination_bucket=final_table_storage_bucket,
                                                         destination_prefix=destination_table_storage_s3_prefix)
                        self.add_partition(table=data_base + "." + table_name,
                                           partition=partition,
                                           bucket=final_table_storage_bucket,
                                           prefix=destination_table_storage_s3_prefix)
                else:
                    self.logger.info("There are no data to move to final table.")

                    """"             
          dropping the staging table after copying the data
          """
            drop_table_query = "drop table {0}.{1}".format(self.staging_database, staging_table_name)
            self.logger.info("Dropping staging table ...{0}".format(drop_table_query))
            query_execution_id = self.submit_athena_query(query_string=drop_table_query)
            self.sync_get_status(query_execution_id)

    def add_partition(self, table, partition, bucket, prefix):
        add_partition_query = "alter table {0} add if not exists partition({1}) location '{2}'"
        location = "s3://" + bucket + "/" + prefix
        partitions = partition[1:].replace("/", ",").replace("=", "='").replace(",", "',") + "'"
        self.logger.info(partitions)
        add_partition_query = add_partition_query.format(table, partitions, location)
        self.logger.info("Add partition query {0}".format(add_partition_query))
        query_execution_id = self.submit_athena_query(add_partition_query)
        self.sync_get_status(query_execution_id)
