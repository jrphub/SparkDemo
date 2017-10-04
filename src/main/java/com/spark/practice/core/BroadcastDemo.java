package com.spark.practice.core;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

public class BroadcastDemo {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("Broadcast_Demo")
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

		Broadcast<List<String>> bstop = jsc.broadcast(stopWords);

		JavaRDD<String> filteredWords = flat_words
				.filter(new Function<String, Boolean>() {

					@Override
					public Boolean call(String word) throws Exception {
						if (bstop.value().contains(word)) {
							return false;
						}
						return true;
					}
				});

		// As the output file is in local, deleting output_dir using FileUtils
		try {
			FileUtils.deleteDirectory(new File(
					"/home/jrp/workspace_1/SparkDemo/output/broadcast_output"));
		} catch (IOException e) {
			e.printStackTrace();
		}

		filteredWords
				.saveAsTextFile("file:///home/jrp/workspace_1/SparkDemo/output/broadcast_output");

		jsc.close();

	}

}
