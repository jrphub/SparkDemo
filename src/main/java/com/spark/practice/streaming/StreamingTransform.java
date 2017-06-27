package com.spark.practice.streaming;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingTransform {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(
				"Streaming_Transform");

		JavaStreamingContext jsc = new JavaStreamingContext(conf,
				Durations.seconds(10)); // Batch Interval

		// set checkpoint directory
		jsc.checkpoint("file:///home/jrp/workspace_1/SparkDemo/ckpt-dir");

		// Receive streaming data from the source
		JavaReceiverInputDStream<String> lines = jsc.socketTextStream(
				"localhost", 9999);

		JavaRDD<String> stopWordsRDD = jsc
				.sparkContext()
				.textFile(
						"file:///home/jrp/workspace_1/SparkDemo/input-data/stopwords_en.txt");

		/*
		 * List<String> words = stopWordsRDD.collect(); for (String word :
		 * words) { System.out.println(word); }
		 * System.out.println(words.size());
		 */

		// Each record in the "lines" stream is a line of text
		// Split each line into words
		JavaDStream<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {
					@Override
					public Iterator<String> call(String line) throws Exception {
						return Arrays.asList(line.split(" ")).iterator();
					}
				});

		JavaDStream<String> noStopWordsStream = words
				.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
					@Override
					public JavaRDD<String> call(JavaRDD<String> rdd)
							throws Exception {
						return rdd.subtract(stopWordsRDD);
					}
				});

		noStopWordsStream.print();

		// Start the computation
		jsc.start();

		// Wait for the computation to terminate
		try {
			jsc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			jsc.close();
		}

	}

}
