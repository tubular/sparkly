## 2.0.0
* Migrate to Spark 2.x, Spark 1.6 isn't supported anymore.
* Rename `SparklyContext` to `SparklySession` and derive it from `SparkSession`.
* Use built-in csv reader.
* Replace `hms` with `catalog_ext`.

## 1.1.1
* Fix: kafka import error.

## 1.1.0
* Kafka reader and writer.
* Kafka fixtures.

## 1.0.0
* Initial open-source release.
* Features:
 - Declarative definition of application dependencies (spark packages, jars, UDFs)
 - Readers and writers for ElasticSearch, Cassandra, MySQL
 - DSL for interaction with Apache Hive Metastore
