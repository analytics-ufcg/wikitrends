package br.edu.ufcg.analytics.wikitrends.vanilla;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.batchview.types.KeyValuePair;
import scala.Tuple2;

public class FullFromMaster {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf();
		conf.setAppName(FullFromMaster.class.getSimpleName());
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		
		JavaRDD<KeyValuePair> serverRanking = javaFunctions(sparkContext).cassandraTable("master_dataset", "edits")
				.select("server_name")
				.filter( row -> row.getString("server_name").endsWith("wikipedia.org"))
				.mapToPair( row -> new Tuple2<String, Long>(row.getString("server_name"), 1L))
//				.map( row -> {
//					EditChange edit = new EditChange();
//					edit.setEventTimestamp(row.getDate("event_timestamp"));
//					edit.setServerName(row.getString("server_name"));
//					return edit;
//				})
//				.filter(edit -> edit.getServerName().endsWith("wikipedia.org"))
//				.mapToPair( edit -> new Tuple2<String, Long>(edit.getServerName(), 1L))
				.reduceByKey( (a,b) -> a+b )
			    .map( tuple -> new KeyValuePair("top_idioms", tuple._1, tuple._2));

		
		CassandraJavaUtil.javaFunctions(serverRanking)
			.writerBuilder("perftest", "fullfrommaster", mapToRow(KeyValuePair.class))
			.saveToCassandra();
	}

}
