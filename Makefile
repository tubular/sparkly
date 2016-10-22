test:
	docker-compose build spark-1.6
	docker-compose run spark-1.6 tox ; retcode="$$?" ; docker-compose down -v ; exit $$retcode
