package br.edu.ufcg.analytics.wikitrends.processing.batch1;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.ArrayList;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditType;
import br.edu.ufcg.analytics.wikitrends.storage.serving1.types.TopClass;
import scala.Tuple2;

public class TopIdiomsBatch1 extends BatchLayer1Job {

	private static final long serialVersionUID = -1738945554412789213L;
	
	private String serversTable;

	public TopIdiomsBatch1(Configuration configuration) {
		super(configuration);
		
		serversTable = configuration.getString("wikitrends.batch.cassandra.table.servers");
	}
	
	@Override
	public void process(JavaSparkContext sc) {
		JavaRDD<EditType> wikipediaEdits = read(sc)
				.filter(edit -> edit.getCommon_server_name().endsWith("wikipedia.org"))
				.cache();
		
		JavaPairRDD<String, Integer> serverRDD = wikipediaEdits
			.mapPartitionsToPair( iterator -> {
				ArrayList<Tuple2<String, Integer>> pairs = new ArrayList<>();
				while(iterator.hasNext()){
					EditType edit = iterator.next();
					pairs.add(new Tuple2<String, Integer>(edit.getCommon_server_name(), 1));
				}
				return pairs;
			});
		
		JavaRDD<TopClass> serverRanking = processRankingEntry(sc, serverRDD);
		
		CassandraJavaUtil.javaFunctions(serverRanking)
			.writerBuilder(getBatchViewsKeyspace(), serversTable, mapToRow(TopClass.class))
			.saveToCassandra();
	}
}
