## 2.5.1
* Fix port issue with reading and writing `by_url`. `urlparse` return `netloc` with port, which breaks read and write from MySQL and Cassandra.

## 2.5.0
* Add `port` argument to `CassandraFixture` and `MysqlFixture`
* Add `Content-Type` header to `ElasticFixture` to support ElasticSearch `6.x`
* Update `elasticsearch-hadoop` connector to `6.5.4`
* Update image tag for elasticsearch to `6.5.4`

## 2.4.1
* Fix write_ext.kafka: run foreachPartition instead of mapPartitions because returned value can cause spark.driver.maxResultSize excess

## 2.4.0
* Respect PYSPARK_SUBMIT_ARGS if it is already set by appending SparklySession related options at the end instead of overwriting.
* Fix additional_options to always override SparklySession.options when a session is initialized
* Fix ujson dependency on environments where redis-py is already installed
* Access or initialize SparklySession through get_or_create classmethod
* Ammend `sparkly.functions.switch_case` to accept a user defined function for
  deciding whether the switch column matches a specific case

## 2.3.0
* Overwrite existing tables in the metastore
* Add functions module and provide switch_case column generation and multijoin
* Add implicit test target import and extended assertEqual variation
* Support writing to redis:// and rediss:// URLs
* Add LRU cache that persists DataFrames under the hood
* Add ability to check whether a complex type defines specific fields

# 2.2.1
* `spark.sql.shuffle.partitions` in `SparklyTest` should be set to string,
because `int` value breaks integration testing in Spark 2.0.2. 

# 2.2.0
* Add instant iterative development mode. `sparkly-testing --help` for more details.
* Use in-memory db for Hive Metastore in `SparklyTest` (faster tests).
* `spark.sql.shuffle.partitions = 4` for `SparklyTest` (faster tests).
* `spark.sql.warehouse.dir = <random tmp dir>` for `SparklyTest` (no side effects)

## 2.1.1
* Fix: remove backtick quoting from catalog utils to ease work with different databases.

## 2.1.0
* Add ability to specify custom maven repositories.

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
