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
import uuid

from sparkly.utils import absolute_path
from sparkly.testing import (
    SparklyGlobalSessionTest,
)
from tests.integration.base import _TestSession


class TestWriteKafka(SparklyGlobalSessionTest):
    session = _TestSession

    def setUp(self):
        self.json_decoder = lambda item: json.loads(item.decode('utf-8'))
        self.json_encoder = lambda item: json.dumps(item).encode('utf-8')
        self.topic = 'test.topic.write.kafka.{}'.format(uuid.uuid4().hex[:10])
        self.fixture_path = absolute_path(__file__, '..', 'integration', 'resources',
                                          'test_write', 'kafka_setup.json',
                                          )
        self.expected_data = self.spark.read.json(self.fixture_path)

    def test_write_kafka_dataframe(self):
        with self.assertRaises(NotImplementedError):
            self.expected_data.write_ext.kafka(
                'kafka.docker',
                self.topic,
                key_serializer=self.json_encoder,
                value_serializer=self.json_encoder,
            )
