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

import json

from sparkly.testing import (
    CassandraFixture,
    MysqlFixture,
    SparklyGlobalSessionTest,
    KafkaFixture)
from tests.integration.base import SparklyTestSession


class TestCassandraFixtures(SparklyGlobalSessionTest):
    session = SparklyTestSession

    def test_cassandra_fixture(self):
        with self.assertRaises(NotImplementedError):
            CassandraFixture(
                'cassandra.docker',
                'test',
                'test',
            )


class TestMysqlFixtures(SparklyGlobalSessionTest):

    session = SparklyTestSession

    def test_mysql_fixture(self):
        with self.assertRaises(NotImplementedError):
            MysqlFixture(
                'mysql.docker',
                'root',
                None,
                'test',
                'test',
            )


class TestKafkaFixture(SparklyGlobalSessionTest):

    session = SparklyTestSession

    def test_kafka_fixture(self):
        with self.assertRaises(NotImplementedError):
            KafkaFixture(
                'kafka.docker',
                topic='test',
                key_serializer=lambda item: json.dumps(item).encode('utf-8'),
                value_serializer=lambda item: json.dumps(item).encode('utf-8'),
                data='test.json',
            )
