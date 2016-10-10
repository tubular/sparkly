FROM registry.tubularlabs.net/emr:4.3.0

ENV SPARK_TESTING true

# testing
RUN pip3 install tox==2.3.1

RUN mkdir /opt/sparkle
WORKDIR /opt/sparkle/

# mysql
RUN yum install -y mysql

# cqlsh
RUN yum install -y python-virtualenv && virtualenv -p python2 venv2 && venv2/bin/pip install pip==8.1.2
RUN venv2/bin/pip install --use-wheel --index-url=https://pypi.tubularlabs.net cassandra-driver==2.7.2
RUN venv2/bin/pip install cqlsh==4.1.1

# Sparkle installation
COPY . /opt/sparkle/
