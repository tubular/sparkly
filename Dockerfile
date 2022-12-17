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

FROM python:3.10

LABEL maintainer="dev@tubularlabs.com"

# Install Java 8
RUN apt-get update && apt-get install -y software-properties-common
RUN apt-add-repository 'deb http://security.debian.org/debian-security stretch/updates main'
RUN apt-get update && apt-get install -y openjdk-8-jdk

# Python env
ENV CASS_DRIVER_NO_EXTENSIONS=1
COPY requirements.txt /tmp/requirements.txt
COPY requirements_dev.txt /tmp/requirements_dev.txt
COPY requirements_extras.txt /tmp/requirements_extras.txt
RUN python -m pip install -r /tmp/requirements.txt
RUN python -m pip install -r /tmp/requirements_dev.txt
RUN python -m pip install -r /tmp/requirements_extras.txt

# Provision Sparkly
ADD . /opt/sparkly/
WORKDIR /opt/sparkly/
