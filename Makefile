APP_NAME := sparkle
APP_PATH := $(shell pwd)
VENV_PATH := $(APP_PATH)/venv3_$(APP_NAME)
PIP_PATH := $(VENV_PATH)/bin/pip
PYTHON_PATH := $(VENV_PATH)/bin/python
SPHINX_PATH := $(VENV_PATH)/bin/sphinx-build

#
# Build
#
dist:	 $(VENV_PATH)/reqs_installed
	$(PYTHON_PATH) setup.py bdist_wheel

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

test:
	docker-compose build sparkle
	docker-compose run sparkle make run_test ; retcode="$$?" ; docker-compose down -v ; exit $$retcode

run_test:
	tox

docs:	$(VENV_PATH)/reqs_installed
	$(PIP_PATH) install Sphinx==1.4.6 sphinxcontrib-napoleon==0.5.3 sphinx-rtd-theme==0.1.9
	$(SPHINX_PATH) -b html docs/source docs/build

.PHONY: test clean lint docs
