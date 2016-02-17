package br.edu.ufcg.analytics.wikitrends.vanilla;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowToTuple;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.batchview.types.RankingEntry;
import scala.Tuple2;

public class SortedFullFromHourly {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf();
		conf.setAppName(SortedFullFromHourly.class.getSimpleName());
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		
		JavaRDD<RankingEntry> serverRanking = javaFunctions(sparkContext)
			    .cassandraTable("batch_views1", "top_editors", mapRowToTuple(String.class, Long.class))
			    .select("name", "count")
			    .mapToPair(row -> new Tuple2<String, Long>(row._1, row._2))
			    .reduceByKey((a,b) -> a+b)
			    .mapToPair( pair -> pair.swap())
			    .sortByKey(false)
			    .zipWithIndex()
			    .map( pair -> new RankingEntry("top_editors", pair._2, pair._1._2, pair._1._1)); 

		
		CassandraJavaUtil.javaFunctions(serverRanking)
			.writerBuilder("perftest", "sortedfullfromhourly", mapToRow(RankingEntry.class))
			.saveToCassandra();
	}

}
