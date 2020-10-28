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
import sys
import os
import re

RUNNING = "RUNNING"
SUCCEEDED = "SUCCEEDED"
QUEUED = "QUEUED"
FAILED = "FAILED"
CANCELLED = "CANCELLED"
S3 = "s3://"
S3A = "s3a://"


class Utils:

    def __init__(self, logger):
        self.logger = logger

    def get_query_string(selt, query_location):
        with open(query_location, "r") as query_file:
            data = query_file.read()
        return data

    def replacing_macro(self, query_string, total_args):
        for x in range(4, total_args):
            macro_input = sys.argv[x]
            macro, macro_value = macro_input.split('=',1)
            macro = "${" + macro + "}"
            self.logger.info("Replacing {0} with {1} ".format(macro, macro_value))
            query_string = query_string.replace(macro, macro_value)
        return query_string

    def validate_all_macros(self, query_string):
        matched_string = re.search(".*\${.*}", query_string)
        if matched_string is not None:
            self.logger.error("Unable to replace the some of the macros value in query {}".format(query_string))
            sys.exit(os.EX_IOERR)

    def split_queries(self, query_string):
        queries = query_string.rstrip().split(";")
        queries = filter(None, queries)
        return queries

    def is_drop_table(self, query):
        result = re.search("^([\s]*)DROP([\s]+)TABLE([\s]+)([.,^.]*)",query,flags=re.IGNORECASE)
        return result

    def is_create_table(self, query):
        result = re.search("^([\s]*)CREATE([\s]+)TABLE([\s]+)([.,^.]*)",query,flags=re.IGNORECASE)
        return result

    def is_insert_into_table(self, query):
        result = re.search("^([\s]*)INSERT([\s]+)INTO([\s]+)([.,^.]*)",query,flags=re.IGNORECASE)
        return result

    def get_table_name_drop_table(self, drop_table_query):
        self.logger.info("drop table query ....." + drop_table_query)
        result = re.sub("^DROP","", drop_table_query.lstrip(), flags=re.IGNORECASE)
        result = re.sub("TABLE","", result, count=1, flags=re.IGNORECASE)
        if result is not None and len(result.strip()) != 0:
            table_name = result.split()[0]
            return table_name
        return None

    def get_database_table(self, table_name):
        try:
            db, table = table_name.split('.')
            return db, table
        except ValueError:
            self.logger.error("Unable to read table name and database from the given string {0}".format(table_name))
            sys.exit(os.EX_IOERR)

    def split_s3_path(self, s3_path):
        path_parts = s3_path.replace(S3, "").replace(S3A, "").split("/")
        s3_bucket = path_parts.pop(0)
        prefix = "/".join(path_parts)
        return s3_bucket, prefix

    def get_table_name_from_insert_query(self, insert_into_query):
        self.logger.info("insert into query....." + insert_into_query)
        result = re.sub("^INSERT","", insert_into_query.lstrip(), flags=re.IGNORECASE)
        result = re.sub("INTO","", result, count=1, flags=re.IGNORECASE)
        if result is not None and len(result.strip()) != 0:
            table_name = result.split()[0]
            return table_name
        return None


    def set_staging_table_property(self,
                                   staging_table_name,
                                   athena_result_location,
                                   table_storage_descriptor,
                                   table_partition_keys):
        staging_s3_location = athena_result_location + staging_table_name
        table_storage_descriptor['Location'] = staging_s3_location
        staging_table_properties = {'Name': staging_table_name,
                                    'StorageDescriptor': table_storage_descriptor,
                                    'TableType': 'EXTERNAL_TABLE',
                                    'PartitionKeys': table_partition_keys}
        return staging_table_properties, staging_s3_location
