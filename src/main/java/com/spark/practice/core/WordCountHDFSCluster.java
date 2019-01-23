package com.spark.practice.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/*

hadoop fs -rm -r /user/huser/wordcountHdfs/output/
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
spark-submit --class com.spark.practice.core.WordCountHDFSCluster --master yarn --deploy-mode cluster --executor-memory 2g spark-uber.jar /user/huser/wordcountHdfs/input-data/wordcount.txt

*/
public class WordCountHDFSCluster {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName(
				"WordCount_Cluster");
		//to run as different user which is a hadoop user, if needed
		System.setProperty("HADOOP_USER_NAME", "huser");

		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		JavaRDD<String> distFile = jsc
				.textFile(args[0]);

		JavaRDD<String> flat_words = distFile
				.flatMap(new FlatMapFunction<String, String>() {
					public Iterator<String> call(String line) throws Exception {
						return Arrays.asList(line.split(" ")).iterator();
					}
				});

		JavaPairRDD<String, Long> flat_words_mapped = flat_words
				.mapToPair(new PairFunction<String, String, Long>() {
					public Tuple2<String, Long> call(String flat_word)
							throws Exception {
						return new Tuple2<String, Long>(flat_word, 1L);
					}
				});

		JavaPairRDD<String, Long> flat_words_reduced = flat_words_mapped
				.reduceByKey(new Function2<Long, Long, Long>() {
					public Long call(Long l1, Long l2) throws Exception {
						return l1 + l2;
					}
				});
		
		// Delete output Directory if exists
		// hadoop fs -rm -r /user/huser/wordcountHdfs/output
		
		// All spark job needs file:// or hdfs:// prefix to distinguish between
		// local and cluster
		
		flat_words_reduced
				.saveAsTextFile("hdfs://localhost:9000/user/huser/wordcountHdfs/output");

		jsc.close();
	}

}
