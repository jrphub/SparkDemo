package com.spark.practice.streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class StreamingKafkaReceiver {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName(
				"Streaming_Kafka_Receiver");

		sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true");
		
		// Create the context with 2 seconds batch size
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
				new Duration(2000));
		jssc.checkpoint("file:///home/jrp/workspace_1/SparkDemo/ckpt-dir");
		
		int numThreads = 2;
		Map<String, Integer> topicMap = new HashMap<>();
		String[] topics = { "test" };
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}


		JavaPairReceiverInputDStream<String, String> messages = org.apache.spark.streaming.kafka.KafkaUtils
				.createStream(jssc, "localhost:2181", "eclipse0", topicMap);
		
		System.out.println("~~~~~");
		messages.print();
		System.out.println("~~~~~");
		
		//topic_name, data
		JavaDStream<String> lines = messages
				.map(new Function<Tuple2<String, String>, String>() {
					@Override
					public String call(Tuple2<String, String> tuple2) {
						return tuple2._2();
					}
				});
		//data
		JavaDStream<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {
					@Override
					public Iterator<String> call(String msg) {
						return Arrays.asList(SPACE.split(msg)).iterator();
					}
				});

		JavaPairDStream<String, Integer> wordCounts = words
				.mapToPair(new PairFunction<String, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<>(s, 1);
					}
				});

		JavaPairDStream<String, Integer> result = wordCounts
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer i1, Integer i2) {
						return i1 + i2;
					}
				});

		wordCounts.print();
		
		
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
			jssc.close();
		}

	}
}
