package br.edu.ufcg.analytics.wikitrends.processing.batch1;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.processing.JobStatusID;
import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditChange;
import br.edu.ufcg.analytics.wikitrends.storage.serving1.types.TopClass;
import scala.Tuple2;

public class TopIdiomsBatch1 extends BatchLayer1Job {

	private static final long serialVersionUID = -1738945554412789213L;
	
	private String idiomsTable;
	private final static JobStatusID TOP_IDIOMS_START_TIME_STATUS_ID = JobStatusID.TOP_IDIOMS_BATCH_1;
	
	public TopIdiomsBatch1(Configuration configuration) {
		super(configuration, TOP_IDIOMS_START_TIME_STATUS_ID);
		
		idiomsTable = configuration.getString("wikitrends.serving1.cassandra.table.idioms");
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

	
}
