package br.edu.ufcg.analytics.wikitrends.processing.batch1;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.ArrayList;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditType;
import br.edu.ufcg.analytics.wikitrends.storage.serving1.types.TopClass;
import scala.Tuple2;

public class TopPagesBatch1 extends BatchLayer1Job {

	private static final long serialVersionUID = 8312361071938353760L;
	
	private String pagesTable;

	public TopPagesBatch1(Configuration configuration) {
		super(configuration);
		
		pagesTable = configuration.getString("wikitrends.batch.cassandra.table.pages");
	}

	@Override
	public void process() {
		JavaRDD<EditType> wikipediaEdits = read()
				.filter(edit -> edit.getCommon_server_name().endsWith("wikipedia.org"))
				.cache();
		
		JavaPairRDD<String, Integer> titleRDD = wikipediaEdits
			.mapPartitionsToPair( iterator -> {
				ArrayList<Tuple2<String, Integer>> pairs = new ArrayList<>();
				while(iterator.hasNext()){
					EditType edit = iterator.next();
					pairs.add(new Tuple2<String, Integer>(edit.getCommon_event_title(), 1));
				}
				return pairs;
			});
		
		JavaRDD<TopClass> titleRanking = transformToTopEntry(titleRDD);
		
		CassandraJavaUtil.javaFunctions(titleRanking)
			.writerBuilder(getBatchViewsKeyspace(), pagesTable, mapToRow(TopClass.class))
			.saveToCassandra();
		
		finalizeSparkContext();
	}
}
