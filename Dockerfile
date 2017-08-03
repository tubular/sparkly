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

MAINTAINER "dev@tubularlabs.com"

# Install OpenJDK 8
RUN apt-get update && apt-get install -y default-jre

# Install Spark 1.6.2
RUN apt-get update && apt-get install -y curl
RUN curl -s http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.7.tgz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s spark-2.1.0-bin-hadoop2.7 spark

ENV SPARK_HOME "/usr/local/spark/"
ENV PYTHONPATH "/usr/local/spark/python/lib/pyspark.zip:/usr/local/spark/python/lib/py4j-0.10.4-src.zip:/opt/sparkly"
ENV SPARK_TESTING true

# Install Python testing utils
RUN apt-get update && apt-get install -y python python3-pip
RUN python3 -m pip install tox==2.4.1

# Remove noisy spark logging
COPY spark.log4j.properties /usr/local/spark/conf/log4j.properties

# Make integration tests faster
RUN /usr/local/spark/bin/spark-shell --repositories=http://packages.confluent.io/maven/ --packages=\
datastax:spark-cassandra-connector:2.0.0-M2-s_2.11,\
org.elasticsearch:elasticsearch-spark-20_2.11:5.4.1,\
org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0,\
mysql:mysql-connector-java:5.1.39,\
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

# Provision Sparkly
ADD . /opt/sparkly/
WORKDIR /opt/sparkly/
