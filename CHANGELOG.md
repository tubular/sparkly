## 0.5.0
* Add managed_table sugar for hql utils.

## 0.4.0
* Add `parallelism` option for writers to control a load on data storages.

## 0.3.2
* Fix: hql.create_table works with decimal and date fields.

## 0.3.1
* Improvement: moved base test classes inside package for reuse in dependant packages.
* Fix: when no partitioning specified for file system writer.
* Fix: remove default values from cassandra reader.

## 0.3.0
* Add generic writer `write.by_url`.
* Refactored `read.by_url`.

## 0.2.1
* Fix empty context initialization bug.

## 0.2.0
* Udfs could be specified in SparkleContext.
* Removed default hardcoded jars (breaking change).

## 0.1.13
* Switch json to ujson

## 0.1.12
* Add support for CSV, Parquet and Hive Metastore tables in `sparkle.read.by_url`.

## 0.1.11
* More hql utils: replace_table, and get_all_table_properties

## 0.1.10
* Kafka reader

## 0.1.9
* Hql get_create_table_statement bug fixes when creating nested complex types

## 0.1.8
* Hql utils: create_table, get_all_tables, table_exists, get_table_property, set_table_property

## 0.1.7
* Fix: direct mapping data frame type to hql
* Fix: backticking field names

## 0.1.6
* Fix: work with timestamp type in hql generator

## 0.1.5
* Generating hive CREATE TABLE by dataframe schema

## 0.1.4
* Read elastic parallelism parameter

## 0.1.3
* Read cassandra parallelism parameter

## 0.1.2
* Fix build to include resources
* Remove cassandra gz from repo
* Fix cassandra write consistency parameter

## 0.1.1
* Generic reader from_url
* Integration testing
* Mysql reader added

## 0.1.0
* Initial version: basic convenient functions and Sparkle context
* Readers for elastic, cassandra, csv