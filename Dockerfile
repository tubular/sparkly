FROM registry.tubularlabs.net/emr:4.3.0

# testing
RUN pip3 install tox==2.3.1

RUN mkdir /opt/sparkle
WORKDIR /opt/sparkle/

# give more ram to java
RUN echo "spark.driver.extraJavaOptions -XX:MaxPermSize=1024m -XX:PermSize=256m" >> $SPARK_HOME/conf/spark-defaults.conf

# cqlsh
RUN yum install -y python-virtualenv && virtualenv -p python2 venv2 && venv2/bin/pip install cassandra-driver==2.7.2, cqlsh==4.1.1

# mysql
RUN yum install -y mysql

# Sparkle installation
ADD . /opt/sparkle/
