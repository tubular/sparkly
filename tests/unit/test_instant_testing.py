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
from unittest import TestCase

try:
    from unittest import mock
except ImportError:
    import mock

from sparkly.instant_testing import InstantTesting


_MOCK_LOCK_FILE_PATH = InstantTesting.LOCK_FILE_PATH + '__test'


@mock.patch.object(InstantTesting, 'LOCK_FILE_PATH', _MOCK_LOCK_FILE_PATH)
class TestInstantTesting(TestCase):
    def setUp(self):
        try:
            os.remove(_MOCK_LOCK_FILE_PATH)
        except:
            pass

    def test_activate(self):
        self.assertFalse(InstantTesting.is_activated())
        InstantTesting.active()
        self.assertTrue(InstantTesting.is_activated())

    def test_deactivate(self):
        InstantTesting.active()
        self.assertTrue(InstantTesting.is_activated())
        InstantTesting.deactivate()
        self.assertFalse(InstantTesting.is_activated())

    def test_double_activation(self):
        InstantTesting.active()
        InstantTesting.active()

    def test_double_deactivation(self):
        InstantTesting.deactivate()
        InstantTesting.deactivate()
