package com.spark.practice.datasets;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class DatasetSchemaMerging {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(
				"Dataset_Schema_Merging");

		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		List<Square> squares = new ArrayList<Square>();
		for (int i = 0; i <= 5; i++) {
			Square square = new Square();
			square.setValue(i);
			square.setSquare(i * i);
			squares.add(square);
		}

		// Create a simple DataFrame, store into a partition directory
		Dataset<Row> squareDf = spark.createDataFrame(squares, Square.class);
		squareDf.write().mode(SaveMode.Overwrite).parquet(
				"file:///home/jrp/workspace_1/SparkDemo/output/test_merge/key=1");
		
		List<Cube> cubes = new ArrayList<Cube>();
		for (int i=6; i<=10; i++) {
			Cube cube = new Cube();
			cube.setCube(i);
			cube.setValue(i * i * i);
			cubes.add(cube);
		}
		
		// Create another DataFrame in a new partition directory,
		// adding a new column and dropping an existing column
		Dataset<Row> cubesDf = spark.createDataFrame(cubes, Cube.class);
		cubesDf.write().mode(SaveMode.Overwrite).parquet("file:///home/jrp/workspace_1/SparkDemo/output/test_merge/key=2");
		
		// Read the partitioned table
		Dataset<Row> mergedDF = spark
				.read()
				.option("mergeSchema", true)
				.parquet(
						"file:///home/jrp/workspace_1/SparkDemo/output/test_merge")
				.orderBy("key");
		mergedDF.show();
		/*+------+-----+----+---+
		|square|value|cube|key|
		+------+-----+----+---+
		|     0|    0|null|  1|
		|     1|    1|null|  1|
		|     4|    2|null|  1|
		|    16|    4|null|  1|
		|    25|    5|null|  1|
		|     9|    3|null|  1|
		|  null|  729|   9|  2|
		|  null|  343|   7|  2|
		|  null| 1000|  10|  2|
		|  null|  216|   6|  2|
		|  null|  512|   8|  2|
		+------+-----+----+---+*/
		
		mergedDF.printSchema();
		/*root
		 |-- square: integer (nullable = true)
		 |-- value: integer (nullable = true)
		 |-- cube: integer (nullable = true)
		 |-- key: integer (nullable = true)*/
		

	}

}
