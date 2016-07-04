FROM registry.tubularlabs.net/emr:4.3.0

# Sparkle installation
ADD . /opt/sparkle/
WORKDIR /opt/sparkle/

RUN echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?><?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?><configuration><property><name>fs.s3a.access.key</name><value>$AWS_ACCESS_KEY_ID</value></property><property><name>fs.s3a.secret.key</name><value>$AWS_SECRET_ACCESS_KEY</value></property></configuration>" >> $SPARK_HOME/conf/hdfs-site.xml
RUN pip3 install -r requirements.txt

# testing
RUN pip3 install tox==2.3.1

# cqlsh
RUN yum install -y python-virtualenv && virtualenv -p python2 venv2 && venv2/bin/pip install cassandra-driver==2.7.2, cqlsh==4.1.1

# mysql
RUN yum install -y mysql