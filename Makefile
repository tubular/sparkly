APP_NAME := sparkle

build: clean
	python3 setup.py bdist_wheel

clean:
	@rm -rf build
	@rm -rf dist
	@rm -rf "$(APP_NAME).egg-info"
	@echo "Cleaned package build artefacts."

publish:
	s3cmd put dist/*.whl s3://pypi.tubularlabs.net/__new/
	curl -XPOST -u c4urself:4d15bedea98f2887b37fad3fb5f8627a http://ci.tubularlabs.net/job/pypi-reindex/build

test:
	echo "I promise to do this later"

.PHONY: build
