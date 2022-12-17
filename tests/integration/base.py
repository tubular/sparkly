#
# Copyright 2017 Tubular Labs, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os

from pyspark.sql.types import StringType

from sparkly import SparklySession
from sparkly.utils import absolute_path


class SparklyTestSession(SparklySession):
    packages = [
        'com.datastax.spark:spark-cassandra-connector_2.12:3.2.0',
        'org.elasticsearch:elasticsearch-spark-30_2.12:7.17.8',
        'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1',
        'mysql:mysql-connector-java:8.0.31',
        'io.confluent:kafka-avro-serializer:3.0.1',
    ]

    repositories = [
        'http://packages.confluent.io/maven/',
    ]

    jars = [
        absolute_path(__file__, 'resources', 'brickhouse-0.7.1.jar'),
    ]

    udfs = {
        'collect': 'brickhouse.udf.collect.CollectUDAF',
        'length_of_text': (lambda text: len(text), StringType())
    }

    options = {
        'my.custom.option.1': '117',
        'my.custom.option.2': 223,
        # will be overwritten by additional_options passed in setup_session
        'my.custom.option.3': '319',
    }
