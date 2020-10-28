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
from configparser import ConfigParser
from s3transfer import RetriesExceededError
from s3transfer.exceptions import TransferNotDoneError
import os
import sys

S3 = "s3://"
S3A = "s3a://"


class ConfigReader:
    def __init__(self, logger, config_location):
        self.config_location = config_location
        self.logger = logger

    def read_config(self):
        config = ConfigParser()
        config.read(self.config_location)
        enable_insert_overwrite = 'True'
        enable_external_table_drop = 'True'
        if 'aws' in config and 'region' in config['aws']:
            aws_region = config['aws']['region']
        else:
            self.logger.error("Not able to read the region from the config ")
            sys.exit(os.EX_CONFIG)
        if 'athena' in config:
            if 'ATHENA_OUTPUT_LOCATION' in config['athena']:
                athena_output_location = config['athena']['ATHENA_OUTPUT_LOCATION']
            else:
                self.logger.error("Not able to read the ATHENA_OUTPUT_LOCATION from the config ")
                sys.exit(os.EX_CONFIG)
            if 'STAGING_DB' in config['athena']:
                staging_db = config['athena']['STAGING_DB']
            else:
                self.logger.error("Not able to read the STAGING_DB from the config ")
                sys.exit(os.EX_CONFIG)
            if 'ENABLE_INSERT_OVERWRITE' in config['athena']:
                enable_insert_overwrite =  config['athena']['ENABLE_INSERT_OVERWRITE']
            if 'ENABLE_EXTERNAL_TABLE_DROP' in config['athena']:
                enable_external_table_drop =  config['athena']['ENABLE_INSERT_OVERWRITE']
        else:
            self.logger.error("Not able to read the athena config")
            sys.exit(os.EX_CONFIG)

        return aws_region, athena_output_location, staging_db, enable_insert_overwrite, enable_external_table_drop


class FileReader:
    def __init__(self, logger, s3_resource):
        self.logger = logger
        self.s3_resource = s3_resource

    def split_s3_path(self, s3_location):
        path_parts = s3_location.replace(S3, "").replace(S3A, "").split("/")
        s3_bucket = path_parts.pop(0)
        prefix = "/".join(path_parts)
        return s3_bucket, prefix

    def download_input_from_s3(self, s3_bucket, prefix, destination_location):
        try:
            self.s3_resource.meta.client.download_file(s3_bucket, prefix, destination_location)
        except RetriesExceededError as e:
            self.logger.fatal("Unable to download the file {0}".format(e))
            self.logger.fatal("Unable to download the file from s3 to local : {0}/{1}".format(s3_bucket, prefix))
            sys.exit(os.EX_DATAERR)
        except TransferNotDoneError as e:
            self.logger.fatal("Unable to download the file {0}".format(e))
            sys.exit(os.EX_OSERR)
        return destination_location

    def get_file(self,file_type, source_location, destination_location):
        if source_location.startswith(S3) or source_location.startswith(S3A):
            self.logger.info("Downloading the {0} from {1} to {2}".format(file_type,
                                                                          source_location,
                                                                          destination_location))
            s3_bucket, prefix = self.split_s3_path(source_location)
            return self.download_input_from_s3(s3_bucket, prefix, destination_location)
        else:
            return source_location
