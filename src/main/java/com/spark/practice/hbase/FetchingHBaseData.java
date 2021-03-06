package com.spark.practice.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import scala.Tuple2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/*

Prerequisites :
create hbase table
create 'imdb_movies', 'movie', 'movie-stats'

Build a jar file and rename it to spark-uber.jar

export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
spark-submit --class com.spark.practice.hbase.FetchingHBaseData --master yarn --deploy-mode cluster --executor-memory 2g --files file:///home/huser/spark-app/params.yml spark-uber.jar params.yml

https://sparkkb.wordpress.com/2015/05/05/read-hbase-table-data-and-create-sql-dataframe-using-spark-api-java-code/

 */
public class FetchingHBaseData {
    public static void main(String[] args) throws IOException {
        //to run as different user which is a hadoop user
        System.setProperty("HADOOP_USER_NAME", "huser");

        String appName = "FetchingHBaseData";
        SparkConf sparkConf = new SparkConf().setAppName(
                appName);
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        SparkSession sparkSession = SparkSession.builder()
                .appName(appName)
                .config(sparkConf)
                .getOrCreate();

        //load external properties
        InputStream input = new FileInputStream(new File(args[0]));
        Yaml yaml = new Yaml(new Constructor(InputParams.class));
        InputParams config = (InputParams) yaml.load(input);

        //create a connection with HBase (Optional)
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
        Configuration conf = newAPIJobConfiguration.getConfiguration();
        conf.set(TableInputFormat.INPUT_TABLE, config.getTableName());
        conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "movie");
        conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, "movie-stats");
        conf.set(TableInputFormat.SCAN_COLUMNS, "movie:title movie:genre movie:description " +
                    "movie:director movie:actors movie:runtime" +
                    " movie-stats:votes movie-stats:revenue movie-stats:rating movie-stats:year");

        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                    jsc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        // in the rowPairRDD the key is hbase's row key, The Row is the hbase's Row data
        JavaPairRDD<String, Movies> rowPairRDD = hBaseRDD.mapToPair(
                new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Movies>() {
                    @Override
                    public Tuple2<String, Movies> call(
                            Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {

                        Result r = entry._2;
                        String keyRow = Bytes.toString(r.getRow());

                        // define java bean
                        Movies mv = new Movies();
                        mv.setRowKey(keyRow);
                        String cf1="movie";
                        String cf2="movie-stats";

                        mv.setTitle(Bytes.toString(r.getValue(Bytes.toBytes(cf1), Bytes.toBytes("title"))));
                        mv.setGenre(Bytes.toString(r.getValue(Bytes.toBytes(cf1), Bytes.toBytes("genre"))));
                        mv.setDescription(Bytes.toString(r.getValue(Bytes.toBytes(cf1), Bytes.toBytes("description"))));
                        mv.setDirector(Bytes.toString(r.getValue(Bytes.toBytes(cf1), Bytes.toBytes("director"))));
                        mv.setActors(Bytes.toString(r.getValue(Bytes.toBytes(cf1), Bytes.toBytes("actors"))));
                        mv.setRuntime(Bytes.toString(r.getValue(Bytes.toBytes(cf1), Bytes.toBytes("runtime"))));

                        mv.setVotes(Bytes.toString(r.getValue(Bytes.toBytes(cf2), Bytes.toBytes("votes"))));
                        mv.setRevenue(Bytes.toString(r.getValue(Bytes.toBytes(cf2), Bytes.toBytes("revenue"))));
                        mv.setRating(Bytes.toString(r.getValue(Bytes.toBytes(cf2), Bytes.toBytes("rating"))));
                        mv.setYear(Bytes.toString(r.getValue(Bytes.toBytes(cf2), Bytes.toBytes("year"))));

                        return new Tuple2<String, Movies>(keyRow, mv);
                    }
                });

        //Now create DataFrame from JavaPairRDD and
        // register temporary table to perform SQL operation.
        // Additionally we can cache and repartition the DataFrame to increase the efficiency.

        Dataset<Row> schemaRDD =   sparkSession.createDataFrame(rowPairRDD.values(), Movies.class);
        //schemaRDD.show();
        schemaRDD.cache(); //persist(MEMORY_ONLY)
        schemaRDD.createOrReplaceTempView(config.getTableName());

        Dataset<Row> queryResultDF = sparkSession.sql(config.getSqlQuery());
        queryResultDF.show();

        //schemaRDD.repartition(100);
        schemaRDD.printSchema();
        jsc.stop();

    }
}
