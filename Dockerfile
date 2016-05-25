FROM registry.tubularlabs.net/emr:4.3.0

# Sparkle installation
ADD . /opt/sparkle/
WORKDIR /opt/sparkle/
RUN pip3 install -r requirements.txt

# testing
RUN pip3 install tox==2.3.1

# cqlsh
RUN tar -xzf tests/integration/resources/dsc-cassandra-2.1.13-bin.tar.gz
ENV PATH $PATH:/opt/sparkle/dsc-cassandra-2.1.13/bin/

# mysql
RUN yum install -y mysql