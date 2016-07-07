APP_NAME := sparkle
APP_PATH := $(shell pwd)
VENV_PATH := $(APP_PATH)/venv3_$(APP_NAME)
PIP_PATH := $(VENV_PATH)/bin/pip
PYTHON_PATH := $(VENV_PATH)/bin/python

dist/%.whl:
	$(PYTHON_PATH) setup.py bdist_wheel

venv:	 $(VENV_PATH)/reqs_installed
build:	dist/%.whl

lint:	$(VENV_PATH)
	VENV=$(VENV_PATH) ../utils/pre-commit-wrapper.py


publish:
	s3cmd put dist/*.whl s3://pypi.tubularlabs.net/__new/
	curl -XPOST -u jenkins:b181da5db5de16a53bc3cd2139f601d8 http://ci.tubularlabs.net/job/pypi-reindex/build

test:
	docker-compose build sparkle
	docker-compose run sparkle make run_test

run_test:
	tox tests

$(VENV_PATH):
	pip install virtualenv
	virtualenv -p $(shell which python3) -q $(VENV_PATH)

clean:
	@rm -rf $(VENV_PATH)
	@rm -rf build
	@rm -rf dist
	@rm -rf "$(APP_NAME).egg-info"
	@echo "Cleaned package build artefacts."

.PHONY: test clean lint
