from unittest import TestCase

import pytest

from sparkly.exceptions import UnsupportedDataType
from sparkly.schema_parser import parse, _generate_structure_type, _process_type


@pytest.mark.branch_1_0
class TestSchemaParser(TestCase):

    def test_struct_parsing(self):
        self.assertEqual(
            parse('a:struct[a:string]|b:list[long]').simpleString(),
            'struct<a:struct<a:string>,b:array<bigint>>',
        )


@pytest.mark.branch_1_0
class TestGenerateSchema(TestCase):

    def test_basic(self):
        res = _generate_structure_type({'field_a': 'long'})
        self.assertEqual(
            res.simpleString(),
            'struct<field_a:bigint>'
        )
        res = _generate_structure_type({'field_a': 'dict[string,long]'})
        self.assertEqual(
            res.simpleString(),
            'struct<field_a:map<string,bigint>>'
        )

        with self.assertRaises(UnsupportedDataType):
            _generate_structure_type({'field_a': 'loooong'})


@pytest.mark.branch_1_0
class TestProcessType(TestCase):

    def test_basic(self):
        self.assertEqual(
            _process_type('string').simpleString(),
            'string',
        )
        self.assertEqual(
            _process_type('list[dict[string,string]]').simpleString(),
            'array<map<string,string>>',
        )
        self.assertEqual(
            _process_type('struct[a:struct[a:string]]').simpleString(),
            'struct<a:struct<a:string>>',
        )
        self.assertEqual(
            _process_type('dict[string,struct[a:dict[long,string]]]').simpleString(),
            'map<string,struct<a:map<bigint,string>>>',
        )
        self.assertEqual(
            _process_type('struct[a:dict[long,dict[string,long]],'
                          'c:dict[long,string]]').simpleString(),
            'struct<a:map<bigint,map<string,bigint>>,c:map<bigint,string>>',
        )
        with self.assertRaises(UnsupportedDataType):
            _process_type('map[string,long]')
