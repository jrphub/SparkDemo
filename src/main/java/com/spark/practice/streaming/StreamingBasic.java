package com.spark.practice.streaming;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class StreamingBasic {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(
				"Streaming_Basic");

		JavaStreamingContext jsc = new JavaStreamingContext(conf,
				Durations.seconds(10)); // Batch Interval

		// Create a DStream that will connect to hostname:port, like
		// localhost:9999
		// This DStream represents streaming data from a TCP source
		// start at terminal : nc -lk 9999
		JavaReceiverInputDStream<String> lines = jsc.socketTextStream(
				"localhost", 9999);
		// Each record in the "lines" stream is a line of text
		// Split each line into words
		//apple orange grape apple orange
		JavaDStream<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {
					@Override
					public Iterator<String> call(String line) throws Exception {
						return Arrays.asList(line.split(" ")).iterator();
					}
				});

		// Count each word in each batch
		// apple 1
		// orange 1
		// grapes 1
		// apple 1
		// orange 1
		JavaPairDStream<String, Long> wordPair = words
				.mapToPair(new PairFunction<String, String, Long>() {
					@Override
					public Tuple2<String, Long> call(String word)
							throws Exception {
						return new Tuple2<String, Long>(word, 1L);
					}
				});
		
		// apple 2
		// orange 2
		// grapes 1
		JavaPairDStream<String, Long> wordCount = wordPair
				.reduceByKey(new Function2<Long, Long, Long>() {

					@Override
					public Long call(Long l1, Long l2) throws Exception {
						return l1 + l2;
					}
				});

		// Print the first ten elements of each RDD generated in this DStream to
		// the console
		
		wordCount.print();

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
