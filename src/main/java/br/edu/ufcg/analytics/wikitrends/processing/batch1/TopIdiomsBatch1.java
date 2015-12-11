package br.edu.ufcg.analytics.wikitrends.processing.batch1;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditChange;
import br.edu.ufcg.analytics.wikitrends.storage.serving1.types.TopClass;
import scala.Tuple2;

public class TopIdiomsBatch1 extends BatchLayer1Job {

	private static final long serialVersionUID = -1738945554412789213L;
	
	private String idiomsTable;
	private final static String TOP_IDIOMS_STATUS_ID = "top_idioms";
	
	public TopIdiomsBatch1(Configuration configuration) {
		super(configuration, TOP_IDIOMS_STATUS_ID);
		
		idiomsTable = configuration.getString("wikitrends.batch.cassandra.table.idioms");
	}
	
	@Override
	public void process() {
		JavaRDD<EditChange> wikipediaEdits = read()
				.filter(edit -> edit.getServerName().endsWith("wikipedia.org"))
				.cache();
		
		JavaPairRDD<String, Integer> serverRDD = wikipediaEdits
			.mapToPair( edit -> new Tuple2<String, Integer>(edit.getServerName(), 1));
		
		JavaRDD<TopClass> serverRanking = transformToTopEntry(serverRDD);
		
		CassandraJavaUtil.javaFunctions(serverRanking)
			.writerBuilder(getBatchViewsKeyspace(), idiomsTable, mapToRow(TopClass.class))
			.saveToCassandra();
	}

	@Override
	public void run2() {
		try (Cluster cluster = Cluster.builder().addContactPoints(getSeeds()).build();
				Session session = cluster.newSession();) {
			
			while(getCurrentTime().isBefore(getStopTime())) {
				process();
			
				session.execute("INSERT INTO batch_views.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
										TOP_IDIOMS_STATUS_ID, 
										getCurrentTime().getYear(), 
										getCurrentTime().getMonthValue(), 
										getCurrentTime().getDayOfMonth(), 
										getCurrentTime().getHour());
				
				this.setCurrentTime(getCurrentTime().plusHours(1));
			}
		} finally {
			finalizeSparkContext();
		}
	}
}
