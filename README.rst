Sparkly
=======

|Sparkly PyPi Version| |Documentation Status|

Helpers & syntax sugar for PySpark. There are several features to make your life easier:

- Definition of spark packages, external jars, UDFs and spark options within your code;
- Simplified reader/writer api for Cassandra, Elastic, MySQL, Kafka;
- Testing framework for spark applications.

More details could be found in `the official
documentation <https://sparkly.readthedocs.org>`__.

Installation
------------

Sparkly itself is easy to install::

    pip install pyspark  # pick your version
    pip install sparkly (compatible with spark >= 2.4)


Getting Started
---------------

Here is a small code snippet to show how to easily read Cassandra table
and write its content to ElasticSearch index::

    from sparkly import SparklySession


    class MySession(SparklySession):
        packages = [
            'datastax:spark-cassandra-connector:2.0.0-M2-s_2.11',
            'org.elasticsearch:elasticsearch-spark-20_2.11:6.5.4',
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

+---------------------------------------------------------------------------+
| sparkly >= 2.7 | Spark 2.4.x                                              |
+---------------------------------------------------------------------------+
| sparkly 2.x    | Spark 2.0.x and Spark 2.1.x and Spark 2.2.x              |
+---------------------------------------------------------------------------+
| sparkly 1.x    | Spark 1.6.x                                              |
+---------------------------------------------------------------------------+

.. |Sparkly PyPi Version| image:: http://img.shields.io/pypi/v/sparkly.svg
   :target: https://pypi.python.org/pypi/sparkly
.. |Sparkly Build Status| image:: https://app.travis-ci.com/tubular/sparkly.svg?branch=master
   :target: https://app.travis-ci.com/github/tubular/sparkly
.. |Documentation Status| image:: https://readthedocs.org/projects/sparkly/badge/?version=latest
   :target: http://sparkly.readthedocs.io/en/latest/?badge=latest
