package com.spark.practice.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

/*

Prerequisites :
create hbase table
create 'imdb_movies', 'movie', 'movie-stats'

Build the project
rename the jar file : spark-uber.jar

export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

spark-submit --class com.spark.practice.hbase.LoadingHBase --master yarn --deploy-mode cluster --executor-memory 2g --files file:///home/huser/spark-app/params.yml spark-uber.jar params.yml

ref : https://medium.com/@sathishjayaram/import-data-from-csv-files-to-hbase-using-spark-1749f395a16b


 */
public class LoadingHBase {
    public static void main(String[] args) throws IOException {
        String appName = "LoadingHbase";
        SparkConf sparkConf = new SparkConf().setAppName(
                appName);
        //to run as different user which is a hadoop user
        System.setProperty("HADOOP_USER_NAME", "huser");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        SparkSession sparkSession = SparkSession.builder()
                .appName(appName)
                .config(sparkConf)
                .getOrCreate();

        //load external properties
        InputStream input = new FileInputStream(new File(args[0]));
        Yaml yaml = new Yaml(new Constructor(InputParams.class));
        InputParams config = (InputParams) yaml.load(input);

        //create a connection with HBase
        Configuration configuration = null;
        try{
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", config.getQuorum());
            configuration.set("hbase.zookeeper.property.clientPort",config.getPort());
            HBaseAdmin.checkHBaseAvailable(configuration);
        } catch (Exception ce){
            ce.printStackTrace();
        }

        //new Hadoop API configuration
        Job newAPIJobConfiguration = Job.getInstance(configuration);
        newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, config.getTableName());
        newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

        //broadcast row key & value data for the Hbase table so the info is
        //available to the worker nodes for processing
        Broadcast<String> ROW_KEY_B = jsc.broadcast(config.getRowKey());//rank:year
        Broadcast<ArrayList<HashMap<String,String>>> ROW_VALUES_B = jsc.broadcast(config.getRowValues());

        //RDD of rows is created from the given CSV file.
        //The data is converted into rows for hbase table based on the schema provided in the params.yml file.
        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(config.getInputFile())
                .javaRDD().mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
                    @Override
                    public Tuple2<ImmutableBytesWritable, Put> call(Row data)
                            throws Exception {
                        //Creating ROWKEY
                        String[] rowKeys =  ROW_KEY_B.value().split(":"); //[rank,year]
                        String key = "";
                        for(String k : rowKeys){
                            key = key + data.getAs(k) + ":";//1:2014:
                        }
                        key = key.substring(0, key.length() - 1);//1:2014
                        Put put = new Put(Bytes.toBytes(key));

                        // creating row value
                        for(HashMap<String,String> val : ROW_VALUES_B.value()){
                            if (val != null) {
                                String[] cq = val.get("qualifier").toString().split(":");//[movie,rank], [movie,title]
                                if (data.getAs(val.get("value")) != null) {
                                    put.add(Bytes.toBytes(cq[0]), Bytes.toBytes(cq[1]),
                                            Bytes.toBytes(data.getAs(val.get("value")).toString()));
                                    //movie,rank,1
                                    //movie,title,abc
                                }

                            }
                        }

                        return new Tuple2<ImmutableBytesWritable, Put>(
                                new ImmutableBytesWritable(), put);
                    }
                }).cache();

        hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration.getConfiguration());

        jsc.stop();

    }
}
