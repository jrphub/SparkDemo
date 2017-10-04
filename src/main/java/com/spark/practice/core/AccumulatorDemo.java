package com.spark.practice.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;

public class AccumulatorDemo {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("Accumulator_Demo")
				.set("spark.executor.instances", "2").setMaster("local[*]");

		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		JavaRDD<String> distFile = jsc
				.textFile("file:///home/jrp/workspace_1/SparkDemo/input-data/wordcount.txt");
		JavaRDD<String> flat_words = distFile
				.flatMap(new FlatMapFunction<String, String>() {
					public Iterator<String> call(String line) throws Exception {
						return Arrays.asList(line.split(" ")).iterator();
					}
				});

		List<String> stopWords = new ArrayList<String>();
		stopWords.add("a");
		stopWords.add("an");
		stopWords.add("the");

		LongAccumulator accum = jsc.sc().longAccumulator("accum_name");
		
		flat_words.foreach(new VoidFunction<String>() {
			
			@Override
			public void call(String word) throws Exception {
				if (stopWords.contains(word)) {
					accum.add(1L);
				}
			}
		});
		
		
		System.out.println(accum.value());

		jsc.close();

	}

}
