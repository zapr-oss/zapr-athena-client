#!/usr/bin/env python
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
import logging
import os
import sys
from zapr.utils.Utils import Utils
from zapr.utils.reader import ConfigReader, FileReader
from zapr.aws.zaprs3client import zaprS3Client
from zapr.aws.Athena import Athena

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

QUERY_FILE = "query.sql"
CONFIG_FILE = "config.ini"


def main():
    total_args = len(sys.argv)
    if total_args < 3:
        logger.info("Client expects at least 3 arguments -> configfile_location workgroup sql_file")
        sys.exit(os.EX_IOERR)
    else:
        config_location = sys.argv[1]
        work_group = sys.argv[2]
        query_location = sys.argv[3]
        logger.info("Given cofig location {0}".format(config_location))
        logger.info("Given QUERY location {0}".format(query_location))

        """Config Reader : download the config file from s3 or read from the local location
            File Reader : Read from the file for query
        """
        zapr_s3_client = zaprS3Client(logger)
        utils = Utils(logger)
        file_reader = FileReader(logger, zapr_s3_client.resource)
        config_location = file_reader.get_file("CONFIG", config_location, CONFIG_FILE)

        config_parser = ConfigReader(logger, config_location)
        region, athena_output_location, staging_db, enable_insert_overwrite, enable_external_table_drop = config_parser.read_config()
        '''Creating an instance for athena'''
        athena = Athena(logger,
                        region,
                        work_group,
                        athena_output_location,
                        staging_db)

        query_location = file_reader.get_file("QUERY", query_location, QUERY_FILE)

        athena_output_location = athena_output_location + work_group + "/"
        logger.debug("STAGING location {0}".format(athena_output_location))

        logger.info("Reading the query...........")
        query_string = utils.get_query_string(query_location)

        logger.info("Replacing the macros with values....")
        query_string = utils.replacing_macro(query_string, total_args)
        utils.validate_all_macros(query_string)

        logger.info("Final query after replacing all the given macro ...\n{0}".format(query_string))
        queries = utils.split_queries(query_string)

        for query in queries:
            logger.info("Submitting the query ... {0}".format(query))

            if utils.is_drop_table(query):
                athena.drop_table_query(query, enable_external_table_drop)

            elif utils.is_create_table(query):
                athena.create_table_query(query)

            elif utils.is_insert_into_table(query):
                athena.insert_into_table_query(query, enable_insert_overwrite)
            else:
                query_execution_id = athena.submit_athena_query(query_string=query)
                athena.sync_get_status(query_execution_id)
        logger.info("query success....")


if __name__ == "__main__":
    main()
