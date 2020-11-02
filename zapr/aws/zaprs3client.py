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
import sys
import os
from botocore.exceptions import ParamValidationError, ClientError


class zaprS3Client:
    client = None
    resource = None

    def __init__(self, logger):
        self.logger = logger
        self.client = boto3.client('s3')
        self.resource = boto3.resource('s3')

    def copy_object(self, source_bucket, source_file_prefix, destination_bucket, destination_file_prefix):
        copy_source = {
            'Bucket': source_bucket,
            'Key': source_file_prefix
        }
        try:
            self.resource.meta.client.copy(copy_source, destination_bucket, destination_file_prefix)
        except self.resource.Client.exceptions.ObjectNotInActiveTierError as e:
            self.logger.fatal("Error occurred while copying object into s3 %e " % e)
            sys.exit(os.EX_OSERR)
        except ParamValidationError as e:
            self.logger.fatal("Parameter validation error in copying object into s3 : %s" % e)
            sys.exit(os.EX_OSERR)
        except ClientError as e:
            self.logger.fatal("Unexpected error in copying object into s3: %s" % e)
            sys.exit(os.EX_OSERR)

    def list_objects(self, bucket, prefix):
        try:
            response = self.client.list_objects(Bucket=bucket, Prefix=prefix)
            return response
        except ParamValidationError as e:
            self.logger.fatal("Parameter validation error in getting objects from s3 : %s" % e)
            sys.exit(os.EX_OSERR)
        except ClientError as e:
            self.logger.fatal("Unexpected error in getting objects from s3 : %s" % e)
            sys.exit(os.EX_OSERR)

    def delete_objects(self, bucket, prefix):
        try:
            if not prefix.endswith('/'):
                prefix = prefix + '/'
            response = self.client.list_objects(
                Bucket=bucket,
                Prefix=prefix)
        except ParamValidationError as e:
            self.logger.fatal("Parameter validation error in getting objects from s3 : %s" % e)
            sys.exit(os.EX_OSERR)
        except ClientError as e:
            self.logger.fatal("Unexpected error in getting objects from s3 : %s" % e)
            sys.exit(os.EX_OSERR)
        if 'Contents' in response:
            for object in response['Contents']:
                print('Deleting', object['Key'])
                try:
                    self.client.delete_object(Bucket=bucket, Key=object['Key'])
                except ParamValidationError as e:
                    self.logger.fatal("Parameter validation error in deleting object from s3 : %s" % e)
                    sys.exit(os.EX_OSERR)
                except ClientError as e:
                    self.logger.fatal("Unexpected error in deleting object from s3 : %s" % e)
                    sys.exit(os.EX_OSERR)
        else:
            self.logger.warn("Not able to delete data in s3 since there is no content ")

    def move_objects(self,
                     source_bucket,
                     source_prefix,
                     destination_bucket,
                     destination_prefix):

        response = self.list_objects(bucket=source_bucket,
                                     prefix=source_prefix)
        if 'Contents' in response:
            list_objects = response['Contents']
            length_objects = len(list_objects)
            if length_objects > 0:
                self.logger.info("Deleting data from Destination location before copying....{0}".format(
                    destination_bucket + "/" + destination_prefix))
                self.delete_objects(destination_bucket, destination_prefix)

                """now moving the data from staging location"""
                self.logger.info("Copying objects from Source to destination location")
                for j in range(length_objects):
                    location = list_objects[j]['Key']
                    file_name = location.replace(source_prefix, "")
                    self.logger.debug("\nPrefix of the file {0}".format(location))
                    self.logger.debug("File name {0}".format(file_name))
                    self.logger.info("Copy....{0}".format(file_name))

                    staging_table_file = source_prefix + file_name
                    destination_table_file = destination_prefix + file_name

                    self.copy_object(source_bucket=source_bucket,
                                     source_file_prefix=staging_table_file,
                                     destination_bucket=destination_bucket,
                                     destination_file_prefix=destination_table_file)
                self.logger.info("Deleting data from source location .....{0}".format(
                    source_prefix + "/" + source_prefix))
                self.delete_objects(source_bucket, source_prefix)
            else:
                self.logger.info("There are no files in the staging table location to copy..Reason : Zero Objects")
        else:
            self.logger.info("There are no contents in the staging table location to copy..Reason : no contents")
