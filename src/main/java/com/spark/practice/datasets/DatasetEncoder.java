package com.spark.practice.datasets;

import java.util.Arrays;
import java.util.Collections;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class DatasetEncoder {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(
				"Dataset_Encoder");

		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		
		// Create an instance of a Bean class
		Person person = new Person();
		person.setName("Andy");
		person.setAge(32);
		
		// Encoders are created for Java beans
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> javaBeanDS = spark.createDataset(
		  Collections.singletonList(person),
		  personEncoder
		);
		javaBeanDS.show();
		
		/*+---+----+
		|age|name|
		+---+----+
		| 32|Andy|
		+---+----+*/
		
		//A usecase
		String path = "file:///home/jrp/workspace_1/Spark21-Example/input-data/people.json";
		Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
		peopleDS.show();
		
		/*+---+-------+
		|age|   name|
		+---+-------+
		| 32|Michael|
		| 60|   Andy|
		| 19| Justin|
		+---+-------+*/
		
		// Encoders for most common types are provided in class Encoders
		Encoder<Integer> integerEncoder = Encoders.INT();
		Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
		Dataset<Integer> transformedDS = primitiveDS.map(new MapFunction<Integer, Integer>() {
		  @Override
		  public Integer call(Integer value) throws Exception {
		    return value + 1;
		  }
		}, integerEncoder);
		transformedDS.show();
		/*
		+-----+
		|value|
		+-----+
		|    2|
		|    3|
		|    4|
		+-----+*/

	}

}
