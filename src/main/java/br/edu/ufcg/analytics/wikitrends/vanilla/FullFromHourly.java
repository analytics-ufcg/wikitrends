package br.edu.ufcg.analytics.wikitrends.vanilla;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowToTuple;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.batchview.types.KeyValuePair;
import scala.Tuple2;

public class FullFromHourly {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf();
		conf.setAppName(FullFromHourly.class.getSimpleName());
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		
		JavaRDD<KeyValuePair> serverRanking = javaFunctions(sparkContext)
			    .cassandraTable("batch_views1", "top_editors", mapRowToTuple(String.class, Long.class))
			    .select("name", "count")
			    .mapToPair(row -> new Tuple2<String, Long>(row._1, row._2))
			    .reduceByKey((a,b) -> a+b)
			    .map( tuple -> new KeyValuePair("top_editors", tuple._1, tuple._2)); 

		
		CassandraJavaUtil.javaFunctions(serverRanking)
			.writerBuilder("perftest", "fullfromhourly", mapToRow(KeyValuePair.class))
			.saveToCassandra();
	}

}
