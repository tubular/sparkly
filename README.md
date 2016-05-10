## Sparkle

Helpers & syntax sugar for PySpark.


### Context definition

The library simplifies usage of spark packages and externals jars.
It was designed as a single place to define project-level dependencies and configuration.
`SparkleContext` is inherited from the `HiveContext`, so you can use it as a regular `HiveContext`.

```
import sparkle


class MyProjectContext(sparkle.SparkleContext):
    # Something that you can find on http://spark-packages.org/
    packages = [
        'org.elasticsearch:elasticsearch-spark_2.10:2.2.0',
    ]
    
    # Connect external UDFs/UDAFs 
    jars = [
        '/path/to/custom/jar/file.jar',
    ]
    
    # Customise Spark options
    options = {
        'spark.sql.shuffle.partitions': 10,
    }
    

hc = MyProjectContext()

hc.read.parquet('path/to/parquet/file')
```

### Accessing data sources

Examples below provide only the basic way of using DataFrame readers and writers,
it is always a good idea to check configuration options provided by the related spark package.

#### Cassandra

Package - http://spark-packages.org/package/datastax/spark-cassandra-connector  
Configuration - https://github.com/datastax/spark-cassandra-connector/blob/b1.5/doc/reference.md  
```
import sparkle
import sparkle.read
import sparkle.write


class MyProjectContext(sparkle.SparkleContext):
    packages = ['datastax:spark-cassandra-connector:1.5.0-M3-s_2.10']
    

hc = MyProjectContext()

# To read data
df = sparkle.read.cassandra(hc, 'primary.cassandra.tubularlabs.net', 'natural', 'facebook_videos')

# To write data
sparkle.write.cassandra(df, 'backup.cassandra.tubularlabs.net', 'natural', 'facebook_videos')
```

#### CSV

Package - http://spark-packages.org/package/databricks/spark-csv  
Configuration - https://github.com/databricks/spark-csv#features  
```
import sparkle
import sparkle.read
import sparkle.write


class MyProjectContext(sparkle.SparkleContext):
    packages = ['com.databricks:spark-csv_2.10:1.4.0']


hc = MyProjectContext()

# To read data
df = sparkle.read.csv(hc, 'path/to/the/csv/file.csv', header=True)

# To write data
sparkle.write.csv(df, 'path/to/the/csv/file.csv', header=False)

```

#### ElasticSearch

Package - http://spark-packages.org/package/elastic/elasticsearch-hadoop  
Configuration - https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html  

```
import sparkle
import sparkle.read
import sparkle.write


class MyProjectContext(sparkle.SparkleContext):
    packages = ['com.databricks:spark-csv_2.10:1.4.0']


hc = MyProjectContext()

# To read data
df = sparkle.read.elastic(
    hc, 'soyuz.elastic.tubularlabs.net', 'intelligence', 'video', fields=['title', 'views'])
    
# To write data
sparkle.write.elastic(df, 'natural.elastic.tubularlabs.net', 'natural', 'facebook_videos')
```

#### Parquet files
  
```
import sparkle

hc = sparkle.SparkleContext()
hc.read.parquet('/path/to/the/parquet/file')
```

#### JSON files

```
import sparkle

hc = sparkle.SparkleContext()
hc.read.json('/path/to/the/json/file')
```

### Utils for testing

```
import sparkle.test


class MyProjectTest(sparkle.test.SparkleTest):
    context = MyProjectContext
    
    def test_some_method(self):
        self.hc.read.parquet('...')
        ...
```
