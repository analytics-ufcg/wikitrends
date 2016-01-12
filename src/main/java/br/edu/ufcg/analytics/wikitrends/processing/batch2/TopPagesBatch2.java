package br.edu.ufcg.analytics.wikitrends.processing.batch2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import org.apache.commons.configuration.Configuration;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.processing.JobStatusID;
import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.TopResult;

public class TopPagesBatch2 extends BatchLayer2Job {
	
	private static final long serialVersionUID = -8448922796164774655L;
	
	private String topPagesTable;
	private final static JobStatusID TOP_PAGES_STATUS_ID = JobStatusID.TOP_PAGES_BATCH_2;
	private final static ProcessResultID TOP_PAGES_PROCESS_RESULT_ID = ProcessResultID.TOP_PAGES;
	
	public TopPagesBatch2(Configuration configuration) {
		super(configuration, TOP_PAGES_STATUS_ID, TOP_PAGES_PROCESS_RESULT_ID);
		
		topPagesTable = configuration.getString("wikitrends.serving2.cassandra.table.toppages");
	}
	
	@Override
	public void process() {
		truncateTable(topPagesTable);
		
		CassandraJavaUtil.javaFunctions(computeFullRankingFromPartial("top_pages"))
		.writerBuilder(getBatchViews2Keyspace(), topPagesTable, mapToRow(TopResult.class))
		.saveToCassandra();
	}

}
