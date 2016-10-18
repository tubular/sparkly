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
    packages = ['org.elasticsearch:elasticsearch-spark_2.10:2.2.0']


hc = MyProjectContext()

# To read data
df = sparkle.read.elastic(
    hc, 'soyuz.elastic.tubularlabs.net', 'intelligence', 'video', fields=['title', 'views'])
    
# To write data
sparkle.write.elastic(df, 'natural.elastic.tubularlabs.net', 'natural', 'facebook_videos')
```

#### Mysql database

```
import sparkle

df = sparkle.read.mysql(hc, 'localhost', 'database', 'table',
                        options={'user': 'root', 'password': 'pass'})
```


#### Generic reader

The main idea of the generic reader is to provide a unified way to define data sources.
The most popular use-case is to path urls as CLI arguments to your program, e.g.:
```
# To use a parquet file on S3 as an input
./my_program.py --input=parquet:s3://....

# To use a cassandra table as an input
./my_program.py --input=cassandra://...
```

Supported formats: 
```
import sparkle.read

metastore_df = sparkle.read.by_url('table://my_hive_metastore_table')
parquet_df = sparkle.read.by_url('parquet:s3://some_bucket/some_parquet/')
csv_df = sparkle.read.by_url('csv:s3://some_bucket/some.csv')
elastic_df = sparkle.read.by_url('elastic://localhost/index_name/type_name?q=name:*Johhny*')
cassandra_df = sparkle.read.by_url('cassandra://localhost/key_space/table_name?consistency=ONE')
mysql_df = sparkle.read.by_url('mysql://localhost/db_name/table_name?user=root&password=pass')
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


### Hql utils

```
from sparkle import SparkleContext
from sparkle.hql import table_manager

hc = SparkleContext()
new_table_df = table_manager(hc, 'my_table').create(df, 's3://path', partition_by=['date']).df()
table_manager(hc).table('old_table').get_property('last_updated')
```

### Documentation

#### Build
```
make docs
open docs/build/index.html
```

