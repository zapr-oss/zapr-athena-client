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
import os
import sys
from botocore.exceptions import ParamValidationError, ClientError


class Glue:
    def __init__(self, logger, region):
        self.logger = logger
        self.client = boto3.client('glue', region_name=region)

    def create_table_in_glue(self, database, table_property):
        try:
            response = self.client.create_table(DatabaseName=database,
                                                TableInput=table_property)
            self.logger.info(response)
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                self.logger.info("Staging table has been created ..... %s" % table_property['Name'])
            else:
                self.logger.fatal("Unexpected response from Glue ..")
                sys.exit(os.EX_OSERR)
        except ParamValidationError as e:
            self.logger.fatal("Parameter validation error: %s" % e)
            sys.exit(os.EX_OSERR)
        except ClientError as e:
            self.logger.fatal("Unexpected error: %s" % e)
            sys.exit(os.EX_OSERR)

    def get_table_data_location(self, database, table):

        """
        get the table properties from the aws glue metastore
         and get the storage location
        """
        table_property = self.get_table_property(database=database,
                                                 table=table)
        if table_property is None:
            self.logger.warn("Table is not present in the AWS Glue..")
            return None
        self.logger.info("Table Properties : {0}".format(table_property))
        # Table storage description
        table_storage_descriptor = table_property['Table']['StorageDescriptor']
        # Output location of the table
        table_data_location = table_storage_descriptor['Location']
        return table_data_location

    def get_table_property(self, database, table):
        try:
            response = self.client.get_table(
                DatabaseName=database,
                Name=table)
            return response
        except ParamValidationError as e:
            self.logger.fatal("Parameter validation error: %s" % e)
            sys.exit(os.EX_OSERR)
        except ClientError as e:
            self.logger.fatal("Unexpected error: %s" % e)
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                self.logger.info("EntityNotFoundException in Aws Glue")
                return None
            else:
                sys.exit(os.EX_OSERR)

    def get_partitions(self, database, table):
        try:
            response = self.client.get_partitions(
                DatabaseName=database,
                TableName=table)
        except ParamValidationError as e:
            self.logger.fatal("Parameter validation error: %s" % e)
            sys.exit(os.EX_OSERR)
        except ClientError as e:
            self.logger.fatal("Unexpected error: %s" % e)
            sys.exit(os.EX_OSERR)
        return response
