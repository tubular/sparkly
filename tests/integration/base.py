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
        'datastax:spark-cassandra-connector:2.4.0-s_2.11',
        'org.elasticsearch:elasticsearch-spark-20_2.11:7.3.0',
        'org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0',
        'mysql:mysql-connector-java:6.0.6',
        'io.confluent:kafka-avro-serializer:3.0.1',
    ]

    repositories = [
        'http://packages.confluent.io/maven/',
    ]

    jars = [
        absolute_path(__file__, 'resources', 'brickhouse-0.7.1.jar'),
    ]

    udfs = {
        'collect_max': 'brickhouse.udf.collect.CollectMaxUDAF',
        'length_of_text': (lambda text: len(text), StringType())
    }


class SparklyTestSessionWithES6(SparklySession):
    packages = [
        'org.elasticsearch:elasticsearch-spark-20_2.11:6.5.4',
    ]

    repositories = [
        'http://packages.confluent.io/maven/',
    ]
