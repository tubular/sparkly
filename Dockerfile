FROM registry.tubularlabs.net/emr:4.3.0

# Sparkle installation
ADD . /opt/sparkle/
WORKDIR /opt/sparkle/

# testing
RUN pip3 install tox==2.3.1

# cqlsh
RUN yum install -y python-virtualenv && virtualenv -p python2 venv2 && venv2/bin/pip install cassandra-driver==2.7.2, cqlsh==4.1.1

# mysql
RUN yum install -y mysql
