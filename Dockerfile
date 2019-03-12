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

FROM ubuntu:16.04

LABEL maintainer="dev@tubularlabs.com"

# Install OpenJDK 8
RUN apt-get update && apt-get install -y default-jre

# Install Spark 2.4.0
RUN apt-get update && apt-get install -y curl
RUN curl -s https://archive.apache.org/dist/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s spark-2.4.0-bin-hadoop2.7 spark

# Install Python development & testing utils
RUN apt-get update && apt-get install -y python python-dev python3-pip
RUN python3 -m pip install tox==2.4.1

# Remove noisy spark logging
COPY spark.log4j.properties /usr/local/spark/conf/log4j.properties

# Make integration tests faster
RUN /usr/local/spark/bin/spark-shell --repositories=http://packages.confluent.io/maven/ --packages=\
datastax:spark-cassandra-connector:2.4.0-s_2.11,\
org.elasticsearch:elasticsearch-spark-20_2.11:6.5.4,\
org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0,\
mysql:mysql-connector-java:6.0.6,\
io.confluent:kafka-avro-serializer:3.0.1

# Python env
RUN apt-get update && apt-get install -y git
ENV CASS_DRIVER_NO_EXTENSIONS=1
COPY requirements.txt /tmp/requirements.txt
COPY requirements_dev.txt /tmp/requirements_dev.txt
COPY requirements_extras.txt /tmp/requirements_extras.txt
RUN python3 -m pip install -r /tmp/requirements.txt
RUN python3 -m pip install -r /tmp/requirements_dev.txt
RUN python3 -m pip install -r /tmp/requirements_extras.txt

ENV SPARK_HOME "/usr/local/spark/"
ENV PYTHONPATH "/usr/local/spark/python/lib/pyspark.zip:/usr/local/spark/python/lib/py4j-0.10.7-src.zip:/opt/sparkly"
ENV SPARK_TESTING true

# Provision Sparkly
ADD . /opt/sparkly/
WORKDIR /opt/sparkly/
