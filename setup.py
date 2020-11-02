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
import setuptools
from setuptools import find_packages

INSTALL_REQUIREMENTS = [
    'boto3==1.13.17',
    'botocore==1.16.17',
]
with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='zapr-athena-client',
    version='0.1',
    author="ZAPR",
    author_email="opensource@gzapr.in",
    description="It is a python library to run the presto query on the AWS Athena.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/zapr-oss/zapr-athena-client",
    packages=setuptools.find_packages(
        include=['zapr*']
    ),
    scripts=['athena_client.py'],
    entry_points={
        "console_scripts": ["zapr-athena-client = athena_client:main"],
    },
    install_requires=INSTALL_REQUIREMENTS,
    classifiers=[
        "Programming Language :: Python :: 3",
        'License :: OSI Approved :: Apache Software License',
        "Operating System :: OS Independent"
    ],
)
