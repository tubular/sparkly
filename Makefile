APP_NAME := sparkle
APP_PATH := $(shell pwd)
VENV_PATH := $(APP_PATH)/venv3_$(APP_NAME)
PIP_PATH := $(VENV_PATH)/bin/pip
PYTHON_PATH := $(VENV_PATH)/bin/python

#
# Build
#
build:	dist/%.whl

dist/%.whl:	 $(VENV_PATH)/reqs_installed
	$(PYTHON_PATH) setup.py bdist_wheel

publish:
	s3cmd put dist/*.whl s3://pypi.tubularlabs.net/__new/
	curl -XPOST -u jenkins:b181da5db5de16a53bc3cd2139f601d8 http://ci.tubularlabs.net/job/pypi-reindex/build

clean:
	@rm -rf $(VENV_PATH)
	@rm -rf build
	@rm -rf dist
	@rm -rf "$(APP_NAME).egg-info"
	@echo "Cleaned package build artefacts."

#
# Test
#
venv:	 $(VENV_PATH)/reqs_installed

$(VENV_PATH):
	pip install virtualenv
	virtualenv -p $(shell which python3) -q $(VENV_PATH)

$(VENV_PATH)/reqs_installed: $(VENV_PATH)
	$(PIP_PATH) install --upgrade pip
	$(PIP_PATH) install --upgrade setuptools
	$(PIP_PATH) install wheel
	$(PIP_PATH) install --default-timeout 60 --use-wheel -i https://pypi.tubularlabs.net -r requirements.txt --no-deps
	touch $(VENV_PATH)/reqs_installed

lint:	$(VENV_PATH)
	VENV=$(VENV_PATH) ../utils/pre-commit-wrapper.py

test:
	docker-compose build sparkle
	docker-compose run sparkle make run_test ; retcode="$$?" ; docker-compose down ; exit $$retcode

run_test:
	tox tests


.PHONY: test clean lint
