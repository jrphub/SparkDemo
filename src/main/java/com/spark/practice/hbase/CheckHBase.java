package com.spark.practice.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class CheckHBase {
    public static void main(String[] args) {
        String appName = "CheckHBase";
        SparkConf sparkConf = new SparkConf().setAppName(
                appName).setMaster("local[*]");
        //to run as different user which is a hadoop user
        System.setProperty("HADOOP_USER_NAME", "huser");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        SparkSession sparkSession = SparkSession.builder()
                .appName(appName)
                .config(sparkConf)
                .getOrCreate();
        Configuration configuration = null;
        try{
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "localhost");
            configuration.set("hbase.zookeeper.property.clientPort","2181");
            HBaseAdmin.checkHBaseAvailable(configuration);
            System.out.println("=====HBase is running=======");
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
