package com.spark.practice.streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class StreamingKafkaDirect {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName(
				"Streaming_Kafka_Direct");

		// Create the context with 2 seconds batch size
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
				new Duration(2000));


		String brokers = "localhost:9092";
		String topics = "test-producer";

		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);


		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils
				.createDirectStream(jssc, String.class, String.class,
						StringDecoder.class, StringDecoder.class, kafkaParams,
						topicsSet);
		//topic_name, data
		JavaDStream<String> lines = messages
				.map(new Function<Tuple2<String, String>, String>() {
					@Override
					public String call(Tuple2<String, String> tuple2) {
						return tuple2._2();
					}
				});

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
