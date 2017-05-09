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

from unittest import TestCase

from sparkly.utils import parse_schema


class TestParseSchema(TestCase):
    def test_atomic(self):
        self.assert_parsed_properly('date')
        self.assert_parsed_properly('float')
        self.assert_parsed_properly('string')
        self.assert_parsed_properly('timestamp')
        self.assert_parsed_properly('int')

    def test_array(self):
        self.assert_parsed_properly('array<int>')

    def test_map(self):
        self.assert_parsed_properly('map<string,float>')

    def test_struct(self):
        self.assert_parsed_properly('struct<a:string,b:float>')

    def test_parse_complex_types(self):
        self.assert_parsed_properly('array<map<string,float>>')
        self.assert_parsed_properly('map<string,struct<a:map<bigint,string>>>')
        self.assert_parsed_properly('struct<a:struct<a:string>>')
        self.assert_parsed_properly('struct<a:map<bigint,map<string,int>>,c:map<int,string>>')

    def assert_parsed_properly(self, schema):
        self.assertEqual(parse_schema(schema).simpleString(), schema)
