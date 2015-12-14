package br.edu.ufcg.analytics.wikitrends.processing.batch1;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.processing.JobStatusID;
import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditChange;
import br.edu.ufcg.analytics.wikitrends.storage.serving1.types.TopClass;
import scala.Tuple2;

public class TopPagesBatch1 extends BatchLayer1Job {

	private static final long serialVersionUID = 8312361071938353760L;

	private static final JobStatusID TOP_PAGES_STATUS_ID = JobStatusID.TOP_PAGES_BATCH_1;
	
	private String pagesTable;

	public TopPagesBatch1(Configuration configuration) {
		super(configuration, TOP_PAGES_STATUS_ID);
		
		pagesTable = configuration.getString("wikitrends.serving1.cassandra.table.pages");
	}

	@Override
	public void process() {
		JavaRDD<EditChange> wikipediaEdits = read()
				.filter(edit -> edit.getServerName().endsWith("wikipedia.org"))
				.cache();
		
		JavaPairRDD<String, Integer> titleRDD = wikipediaEdits
			.mapToPair( edit -> new Tuple2<String, Integer>(edit.getTitle(), 1));
		
		JavaRDD<TopClass> titleRanking = transformToTopEntry(titleRDD);
		
		CassandraJavaUtil.javaFunctions(titleRanking)
			.writerBuilder(getBatchViewsKeyspace(), pagesTable, mapToRow(TopClass.class))
			.saveToCassandra();
		
	}

	@Override
	public void run2() {
		try (Cluster cluster = Cluster.builder().addContactPoints(getSeeds()).build();
				Session session = cluster.newSession();) {
			
			while(getCurrentTime().isBefore(getStopTime())) {
				new TopIdiomsBatch1(configuration).process();
			
				session.execute("INSERT INTO batch_views.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
										TOP_PAGES_STATUS_ID, 
										getCurrentTime().getYear(), 
										getCurrentTime().getMonthValue(), 
										getCurrentTime().getDayOfMonth(), 
										getCurrentTime().getHour());
				
				this.setCurrentTime(getCurrentTime().plusHours(1));
			}
		}
	}
}
