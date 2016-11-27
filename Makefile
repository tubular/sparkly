dev:
	docker-compose run dev-spark-1.6 bash

dist:
	docker-compose build dev
	docker-compose run --no-deps dev python3 setup.py bdist_wheel ; retcode="$$?" ; docker-compose down -v ; exit $$retcode

docs:
	docker-compose run --no-deps dev sphinx-build -b html docs/source docs/build

test:
	docker-compose build test-spark-1.6
	docker-compose run test-spark-1.6 tox ; retcode="$$?" ; docker-compose down -v ; exit $$retcode

.PHONY: docs
