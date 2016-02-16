package br.edu.ufcg.analytics.wikitrends.processing.batch2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.Arrays;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaRDD;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.processing.JobStatusID;
import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.KeyValuePair;
import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.RankingEntry;

public class TopIdiomsBatch2 extends BatchLayer2Job {
	
	private static final long serialVersionUID = 6811359470576431827L;

	private String topIdiomsTable;
	
	private final static JobStatusID TOP_IDIOMS_STATUS_ID = JobStatusID.TOP_IDIOMS_BATCH_2;
	private final static ProcessResultID TOP_IDIOMS_PROCESS_RESULT_ID = ProcessResultID.TOP_IDIOMS;
	
	public TopIdiomsBatch2(Configuration configuration) {
		super(configuration, TOP_IDIOMS_STATUS_ID, TOP_IDIOMS_PROCESS_RESULT_ID);
		
		topIdiomsTable = configuration.getString("wikitrends.serving2.cassandra.table.topidioms");
	}
	
	@Override
	public void process() {
		JavaRDD<RankingEntry> fullRanking = computeFullRankingFromPartial("top_idioms");
		CassandraJavaUtil.javaFunctions(fullRanking)
			.writerBuilder(getBatchViews2Keyspace(), "rankings", mapToRow(RankingEntry.class))
			.saveToCassandra();
		
		JavaRDD<KeyValuePair> distinctRDD = getJavaSparkContext().parallelize(Arrays.asList(new KeyValuePair("absolute_values", "distinct_servers_count", fullRanking.count())));
		
		CassandraJavaUtil.javaFunctions(distinctRDD)
		.writerBuilder(getBatchViews2Keyspace(), "absolute_values", mapToRow(KeyValuePair.class))
		.saveToCassandra();

	}

}