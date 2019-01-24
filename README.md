#### README

To run Hbase programs,

build the project using

```shell
mvn clean install
```

Then rename target/SparkDemo*.jar to uber.jar

```shell
mv target/SparkDemo-0.0.1-SNAPSHOT.jar spark-uber.jar
```

Set environment variable

```shell
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
```

Change the properties in params.yml as per your environment.

You can put input file (imdb_movies_data.csv) in hdfs and update the path in params.yml

To load csv data into HBase table

```shell
spark-submit --class com.spark.practice.hbase.LoadingHBase --master yarn --deploy-mode cluster --executor-memory 2g --files file:///home/huser/spark-app/params.yml spark-uber.jar params.yml
```

The same can be done in local mode as well.

```shell
spark-submit --class com.spark.practice.hbase.LoadingHBase --master local[*] --executor-memory 2g --files file:///home/huser/spark-app/params.yml spark-uber.jar params.yml
```

To fetch or play with data, you can modify the sqlQuery parameter in params.yml file

```shell
spark-submit --class com.spark.practice.hbase.FetchingHBaseData --master yarn --deploy-mode cluster --executor-memory 2g --files file:///home/huser/spark-app/params.yml spark-uber.jar params.yml
```

The same can be run with --master local[*]

Sample Query:

To get top 10 movies of year 2012

sqlQuery: "select title,rating from imdb_movies_demo01 where year='2012' order by rating desc limit 10"

**Note**

"Update" is not supported using spark sql.

Only allowed options are

```
{'(', 'SELECT', 'FROM', 'ADD', 'DESC', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'INSERT', 'DELETE', 'DESCRIBE', 'EXPLAIN', 'SHOW', 'USE', 'DROP', 'ALTER', 'MAP', 'SET', 'RESET', 'START', 'COMMIT', 'ROLLBACK', 'REDUCE', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'DFS', 'TRUNCATE', 'ANALYZE', 'LIST', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'EXPORT', 'IMPORT', 'LOAD'}
```

