package br.edu.ufcg.analytics.wikitrends.processing.batch1;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.time.LocalDateTime;
import java.util.ArrayList;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditChange;
import br.edu.ufcg.analytics.wikitrends.storage.serving1.types.TopClass;
import scala.Tuple2;

public class TopContentPagesBatch1 extends BatchLayer1Job {

	private static final long serialVersionUID = 5005439419731611631L;

	private static final String TOP_CONTENT_PAGES_STATUS_ID = "top_content_pages";

	private String contentPagesTable;

	public TopContentPagesBatch1(Configuration configuration) {
		super(configuration, TOP_CONTENT_PAGES_STATUS_ID);
		
		contentPagesTable = configuration.getString("wikitrends.batch.cassandra.table.contentpages");
	}

	public void process() {
		JavaRDD<EditChange> wikipediaEdits = read()
				.filter(edit -> edit.getServerName().endsWith("wikipedia.org"))
				.cache();
		
		JavaPairRDD<String, Integer> contentTitleRDD = wikipediaEdits
			.filter(edits -> 0 == edits.getNamespace())
			.mapPartitionsToPair( iterator -> {
				ArrayList<Tuple2<String, Integer>> pairs = new ArrayList<>();
				while(iterator.hasNext()){
					EditChange edit = iterator.next();
					pairs.add(new Tuple2<String, Integer>(edit.getTitle(), 1));
				}
				return pairs;
			});
		
		JavaRDD<TopClass> contentTitleRanking = transformToTopEntry(contentTitleRDD);
		
		CassandraJavaUtil.javaFunctions(contentTitleRanking)
			.writerBuilder(getBatchViewsKeyspace(), contentPagesTable, mapToRow(TopClass.class))
			.saveToCassandra();
	}
	
	@Override
	public void run2() {
		try (Cluster cluster = Cluster.builder().addContactPoints(getSeeds()).build();
				Session session = cluster.newSession();) {
			
			while(getCurrentTime().isBefore(getStopTime())) {
				new TopIdiomsBatch1(configuration).process();
			
				session.execute("INSERT INTO batch_views.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
										TOP_CONTENT_PAGES_STATUS_ID, 
										getCurrentTime().getYear(), 
										getCurrentTime().getMonthValue(), 
										getCurrentTime().getDayOfMonth(), 
										getCurrentTime().getHour());
				
				this.setCurrentTime(getCurrentTime().plusHours(1));
			}
		}
	}
}
