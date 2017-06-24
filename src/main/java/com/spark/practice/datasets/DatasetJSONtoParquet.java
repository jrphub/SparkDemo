package com.spark.practice.datasets;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class DatasetJSONtoParquet {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]")
                .setAppName("Dataset_JSON_to_Parquet");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        Dataset<Row> peopleDF = spark.read().format("json").load(
                "file:///home/jrp/workspace_1/SparkDemo/input-data/people.json");
        peopleDF.select("name", "age").write().mode(SaveMode.Overwrite).format("parquet")
                .save("file:///home/jrp/workspace_1/SparkDemo/input-data/people.parquet");
        
        Dataset<Row> sqlDF =
                spark.sql("SELECT * FROM parquet.`file:///home/jrp/workspace_1/SparkDemo/input-data/people.parquet`");
        sqlDF.show();
        
        /*+-------+---+
        |   name|age|
        +-------+---+
        |Michael| 32|
        |   Andy| 60|
        | Justin| 19|
        +-------+---+*/
        
        peopleDF.createOrReplaceTempView("peopleParquet");
        Dataset<Row> sqlDFTwo = spark.sql("select name from peopleParquet where age between 13 and 19");
        sqlDFTwo.show();
        
        Dataset<String> sqlDS = sqlDFTwo.map(new MapFunction<Row, String>() {
        	@Override
        	public String call(Row row) throws Exception {
        		return "first name : " + row.getAs("name");
        	}
		}, Encoders.STRING());
        
        sqlDS.show();

    }

}
