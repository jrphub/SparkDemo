package com.spark.practice.streaming;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class StreamingUpdateStateByKey {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]")
                .setAppName("Streaming_UpdateStateByKey");

        JavaStreamingContext jsc = new JavaStreamingContext(conf,
                Durations.seconds(10)); // Batch Interval

        // set checkpoint directory
        jsc.checkpoint("C:\\ws\\fae-ws-spark16\\SparkDemo\\ckpt-dir");

        // Receive streaming data from the source
        JavaReceiverInputDStream<String> lines = jsc
                .socketTextStream("localhost", 9999);

        // Each record in the "lines" stream is a line of text
        // Split each line into words
        JavaDStream<String> words = lines
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String line) throws Exception {
                        return Arrays.asList(line.split(" ")).iterator();
                    }
                });

        // Count each word in each batch
        JavaPairDStream<String, Long> wordPair = words
                .mapToPair(new PairFunction<String, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(String word)
                            throws Exception {
                        return new Tuple2<String, Long>(word, 1L);
                    }
                });
        //scala code
        /*def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
                val newCount = ...  // add the new values with the previous running count to get the new count
                Some(newCount)
            }
            val runningCounts = pairs.updateStateByKey[Int](updateFunction _)
        */
        JavaPairDStream<String, Long> runningCounts = wordPair.updateStateByKey(
                new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
                    public Optional<Long> call(List<Long> values,
                            Optional<Long> state) throws Exception {
                        Long currentSum = state.orElse(0L); // state != null ?
                                                            // state : OL;
                        for (Long value : values) {
                            currentSum = currentSum + value;
                        }
                        return Optional.of(currentSum);
                    };
                });

        runningCounts.print();

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
