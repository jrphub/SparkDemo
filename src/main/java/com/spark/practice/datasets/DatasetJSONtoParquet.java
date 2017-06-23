package com.spark.practice.datasets;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
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

    }

}
