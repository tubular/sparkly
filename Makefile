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

dev:
	docker-compose run dev-spark-1.6 bash

dist:
	docker-compose build dev-spark-1.6
	docker-compose run --no-deps dev-spark-1.6 python3 setup.py bdist_wheel ; retcode="$$?" ; docker-compose down -v ; exit $$retcode

docs:
	docker-compose run --no-deps dev-spark-1.6 sphinx-build -b html docs/source docs/build

test:
	docker-compose build test-spark-1.6
	docker-compose run test-spark-1.6 tox ; retcode="$$?" ; docker-compose down -v ; exit $$retcode

.PHONY: docs
