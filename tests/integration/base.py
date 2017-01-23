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

from pyspark.sql.types import StringType

from sparkly import SparklySession
from sparkly.utils import absolute_path


class _TestSession(SparklySession):
    packages = [
        'datastax:spark-cassandra-connector:2.0.0-M2-s_2.11',
        'org.elasticsearch:elasticsearch-spark-20_2.11:5.1.1',
        'org.apache.spark:spark-streaming-kafka-0-10_2.11:2.1.0',
    ]

    jars = [
        absolute_path(__file__, 'resources', 'brickhouse-0.7.1.jar'),
        absolute_path(__file__, 'resources', 'mysql-connector-java-5.1.39-bin.jar'),
    ]

    udfs = {
        'collect_max': 'brickhouse.udf.collect.CollectMaxUDAF',
        'length_of_text': (lambda text: len(text), StringType())
    }
