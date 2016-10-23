FROM ubuntu:16.04

MAINTAINER "dev@tubularlabs.com"

# Install OpenJDK 8
RUN apt-get update && apt-get install -y default-jre

# Install Spark 1.6.2
RUN apt-get install -y curl
RUN curl -s http://d3kbcqa49mib13.cloudfront.net/spark-1.6.2-bin-hadoop2.6.tgz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s spark-1.6.2-bin-hadoop2.6 spark

ENV SPARK_HOME "/usr/local/spark/"
ENV PYTHONPATH "/usr/local/spark/python/lib/pyspark.zip:/usr/local/spark/python/lib/py4j-0.9-src.zip"
ENV SPARK_TESTING true

# Install Python testing utils
RUN apt-get install -y python python3-pip
RUN python3 -m pip install tox==2.4.1

# Temporary solution to make integration tests
RUN virtualenv -p python2 venv2 && /venv2/bin/pip install pip==8.1.2
RUN /venv2/bin/pip install --use-wheel --index-url=https://pypi.tubularlabs.net cassandra-driver==2.7.2
RUN /venv2/bin/pip install cqlsh==4.1.1
RUN apt-get install -y mysql-client

# Remove noisy spark logging
COPY spark.log4j.properties /usr/local/spark/conf/log4j.properties

# Make integration tests faster
RUN /usr/local/spark/bin/spark-shell --packages=\
com.databricks:spark-csv_2.10:1.4.0,\
datastax:spark-cassandra-connector:1.6.1-s_2.10,\
org.elasticsearch:elasticsearch-spark_2.10:2.3.0

# Provision Sparkle
ADD . /opt/sparkle/
WORKDIR /opt/sparkle/
