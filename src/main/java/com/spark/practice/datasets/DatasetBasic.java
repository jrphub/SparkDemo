package com.spark.practice.datasets;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

public class DatasetBasic {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(
				"Dataset_Basic");
		
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		Dataset<Row> df = spark
				.read()
				.json("file:///home/jrp/workspace_1/SparkDemo/input-data/people.json");
		df.show();
		/*+---+-------+
		|age|   name|
		+---+-------+
		| 32|Michael|
		| 60|   Andy|
		| 19| Justin|
		+---+-------+*/
		
		df.printSchema();
		/*
		root
		 |-- age: long (nullable = true)
		 |-- name: string (nullable = true)*/
		
		df.select("name").show();
		/*+-------+
		|   name|
		+-------+
		|Michael|
		|   Andy|
		| Justin|
		+-------+*/
		
		//https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/functions.html
		df.select(functions.col("name"), functions.col("age").plus(1)).show();
		/*
		+-------+---------+
		|   name|(age + 1)|
		+-------+---------+
		|Michael|       33|
		|   Andy|       61|
		| Justin|       20|
		+-------+---------+*/
		
		df.filter(functions.col("age").gt(21)).show();
		/*
		+---+-------+
		|age|   name|
		+---+-------+
		| 32|Michael|
		| 60|   Andy|
		+---+-------+*/
		
		df.groupBy("age").count().show();
		/*
		+---+-----+
		|age|count|
		+---+-----+
		| 19|    1|
		| 32|    1|
		| 60|    1|
		+---+-----+*/
		
		//Running queries programmatically
		
		/*
		 * The sql function on a SparkSession enables applications to run SQL
		 * queries programmatically and returns the result as a Dataset<Row>.
		 */
		
		df.createOrReplaceTempView("people");

		Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
		sqlDF.show();
		
		/*+---+-------+
		|age|   name|
		+---+-------+
		| 32|Michael|
		| 60|   Andy|
		| 19| Justin|
		+---+-------+*/
		
		//Global Temporary view
		
		/*
		 * Temporary views in Spark SQL are session-scoped and will disappear if
		 * the session that creates it terminates. If you want to have a
		 * temporary view that is shared among all sessions and keep alive until
		 * the Spark application terminates, you can create a global temporary
		 * view. Global temporary view is tied to a system preserved database
		 * global_temp
		 * 
		 */
		
		try {
			df.createGlobalTempView("peopleGlobal");
		} catch (AnalysisException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		spark.sql("SELECT * FROM global_temp.peopleGlobal").show();
		
		/*+---+-------+
		|age|   name|
		+---+-------+
		| 32|Michael|
		| 60|   Andy|
		| 19| Justin|
		+---+-------+*/

	}

}
