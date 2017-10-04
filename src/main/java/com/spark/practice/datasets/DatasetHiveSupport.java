package com.spark.practice.datasets;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DatasetHiveSupport {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setMaster("local[*]")
				.setAppName("Dataset_Hive_Support")
				//Not using hive set up via hive-site.xml, core-site.xml
				.set("spark.sql.warehouse.dir",
						"file:///home/jrp/workspace_1/SparkDemo/spark-warehouse");
				//This will create metastore_db in project directory

		SparkSession spark = SparkSession.builder().config(conf)
				.enableHiveSupport().getOrCreate();

		spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
		spark.sql("SHOW TABLES").show();
		/*+--------+---------+-----------+
		|database|tableName|isTemporary|
		+--------+---------+-----------+
		| default|      src|      false|
		+--------+---------+-----------+*/
		
		spark.sql("LOAD DATA LOCAL INPATH 'file:///home/jrp/workspace_1/SparkDemo/input-data/kv1.parquet' OVERWRITE INTO TABLE src");

		spark.sql("select * from src").show();
		/*+---+-------+
		|key|  value|
		+---+-------+
		|238|val_238|
		| 86| val_86|
		|311|val_311|
		| 27| val_27|
		|165|val_165|
		|409|val_409|
		|255|val_255|
		|278|val_278|
		| 98| val_98|
		|484|val_484|
		|265|val_265|
		|193|val_193|
		+---+-------+*/

		spark.sql("SELECT COUNT(*) as count FROM src").show();
		/*+-----+
		|count|
		+-----+
		|   12|
		+-----+*/
		
		// The results of SQL queries are themselves DataFrames and support all normal functions.
		Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 200 ORDER BY key");
		
		Dataset<String> srcDS = sqlDF.map(new MapFunction<Row, String>() {
			@Override
			public String call(Row row) throws Exception {
				return "key :" + row.getAs("key") + ", value : " + row.getAs("value");
			}
		}, Encoders.STRING());
		
		srcDS.show();
		
		/*+--------------------+
		|               value|
		+--------------------+
		|key :27, value : ...|
		|key :86, value : ...|
		|key :98, value : ...|
		|key :165, value :...|
		|key :193, value :...|
		+--------------------+*/
		
		// You can also use DataFrames to create temporary views within a SparkSession.
		List<RecordHive> records = new ArrayList<>();
		for (int key = 1; key < 100; key++) {
			RecordHive record = new RecordHive();
		  record.setKey(key);
		  record.setValue("val_" + key);
		  records.add(record);
		}
		Dataset<Row> recordsDF = spark.createDataFrame(records, RecordHive.class);
		recordsDF.createOrReplaceTempView("records");

		// Queries can then join DataFrames data with data stored in Hive.
		spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show();
		/*+---+------+---+------+
		|key| value|key| value|
		+---+------+---+------+
		| 86|val_86| 86|val_86|
		| 27|val_27| 27|val_27|
		| 98|val_98| 98|val_98|
		+---+------+---+------+*/
	}

}
