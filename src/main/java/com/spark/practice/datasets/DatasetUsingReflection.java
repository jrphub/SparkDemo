package com.spark.practice.datasets;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DatasetUsingReflection {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]")
                .setAppName("Dataset_Reflection");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        JavaRDD<Person> peopleRDD = spark.read()
                .textFile(
                        "file:///home/jrp/workspace_1/SparkDemo/input-data/people.csv")
                .javaRDD().map(new Function<String, Person>() {
                    public Person call(String line) throws Exception {
                        String[] cols = line.split(",");

                        Person person = new Person();
                        person.setName(cols[0]);
                        person.setAge(Integer.parseInt(cols[1]));
                        return person;
                    };

                });

        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD,
                Person.class);

        peopleDF.createOrReplaceTempView("people");
        peopleDF.show();

        Dataset<Row> teenagersDF = spark
                .sql("select name from people where age between 13 and 19");

        teenagersDF.show();

        // want to add 2 years to each people's age
        // So, need to access each field
        // 1. by field index

        Dataset<Integer> agesByIndex = peopleDF
                .map(new MapFunction<Row, Integer>() {
                    @Override
                    public Integer call(Row row) throws Exception {
                        int age = (int) row.get(0) + 2; // columns are order
                                                        // lexicographically
                        return age;
                    }
                }, Encoders.INT());

        agesByIndex.show();

        // 2. by column name
        Dataset<Integer> agesByColName = peopleDF
                .map(new MapFunction<Row, Integer>() {
                    @Override
                    public Integer call(Row row) throws Exception {
                        return row.<Integer> getAs("age") + 2;
                    }
                }, Encoders.INT());

        agesByColName.show();

    }

}
