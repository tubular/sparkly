## 2.0.4
* Make it possible to override default value of spark.sql.catalogImplementation

## 2.0.3
* Add KafkaWatcher to facilitate testing of writing to Kafka
* Fix a few minor pyflakes warnings and typos

## 2.0.2
* Fix: #40 write_ext.kafka ignores errors.

## 2.0.1
* Migrate to Spark 2, Spark 1.6.x isn't supported by sparkly 2.x.
* Rename `SparklyContext` to `SparklySession` and derive it from `SparkSession`.
* Use built-in csv reader.
* Replace `hms` with `catalog_ext`.
* `parse_schema` is now consistent with `DataType.simpleString` method.

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
