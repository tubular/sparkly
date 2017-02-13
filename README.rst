Sparkly
=======

|Sparkly PyPi Version| |Sparkly Build Status| |Documentation Status|

Helpers & syntax sugar for PySpark. There are several features to make your life easier:

- Definition of spark packages, external jars, UDFs and spark options within your code;
- Simplified reader/writer api for Cassandra, Elastic, MySQL, Kafka;
- Testing framework for spark applications.

More details could be found in `the official
documentation <https://sparkly.readthedocs.org>`__.

Installation
------------

Sparkly itself is easy to install::

    pip install sparkly

The tricky part is ``pyspark``. There is no official distribution on
PyPI. As a workaround we can suggest:

1) Use env variable ``PYTHONPATH`` to point to your Spark installation,
   something like::

       export PYTHONPATH="/usr/local/spark/python/lib/pyspark.zip:/usr/local/spark/python/lib/py4j-0.10.4-src.zip"

2) Use our ``setup.py`` file for ``pyspark``. Just add this to your
   ``requirements.txt``::

       -e git+https://github.com/Tubular/spark@branch-2.1.0#egg=pyspark&subdirectory=python

Here in Tubular, we published ``pyspark`` to our internal PyPi
repository.

Getting Started
---------------

Here is a small code snippet to show how to easily read Cassandra table
and write its content to ElasticSearch index::

    from sparkly import SparklySession


    class MySession(SparklySession):
        packages = [
            'datastax:spark-cassandra-connector:2.0.0-M2-s_2.11',
            'org.elasticsearch:elasticsearch-spark-20_2.11:5.1.1',
        ]
        

    if __name__ == '__main__':
        spark = MySession()
        df = spark.read_ext.cassandra('localhost', 'my_keyspace', 'my_table')
        df.write_ext.elastic('localhost', 'my_index', 'my_type')

See `the online documentation <https://sparkly.readthedocs.org>`__ for
more details.

Testing
-------

To run tests you have to have `docker <https://www.docker.com/>`__ and
`docker-compose <https://docs.docker.com/compose/>`__ installed on your
system. If you are working on MacOS we highly recommend you to use
`docker-machine <https://docs.docker.com/machine/>`__. As soon as the
tools mentioned above have been installed, all you need is to run::

    make test

Supported Spark Versions
------------------------

At the moment we support:

+-------------------------------------------+
| sparkly 2.x | Spark 2.0.x and Spark 2.1.x |
+-------------------------------------------+
| sparkly 1.x | Spark 1.6.x                 |
+-------------------------------------------+

.. |Sparkly PyPi Version| image:: http://img.shields.io/pypi/v/sparkly.svg
   :target: https://pypi.python.org/pypi/sparkly
.. |Sparkly Build Status| image:: https://travis-ci.org/Tubular/sparkly.svg?branch=master
   :target: https://travis-ci.org/Tubular/sparkly
.. |Documentation Status| image:: https://readthedocs.org/projects/sparkly/badge/?version=latest
   :target: http://sparkly.readthedocs.io/en/latest/?badge=latest
