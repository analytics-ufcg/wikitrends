package br.edu.ufcg.analytics.wikitrends.processing.batch2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import org.apache.commons.configuration.Configuration;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.processing.JobStatusID;
import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.TopResult;

public class TopContentPagesBatch2 extends BatchLayer2Job {
	
	private static final long serialVersionUID = 5181147901979329455L;
	
	private String topContentPagesTable;
	
	private final static JobStatusID TOP_CONTENT_PAGES_STATUS_ID = JobStatusID.TOP_CONTENT_PAGES_BATCH_2;
	private final static ProcessResultID TOP_CONTENT_PAGES_PROCESS_RESULT_ID = ProcessResultID.TOP_CONTENT_PAGES;
	
	public TopContentPagesBatch2(Configuration configuration)  {
		super(configuration, TOP_CONTENT_PAGES_STATUS_ID, TOP_CONTENT_PAGES_PROCESS_RESULT_ID);
		
		topContentPagesTable = configuration.getString("wikitrends.serving2.cassandra.table.topcontentpages");
	}
	
	@Override
	public void process() {
		truncateResultingTable(topContentPagesTable);
		
		CassandraJavaUtil.javaFunctions(computeFullRankingFromPartial("top_content_pages"))
			.writerBuilder(getBatchViews2Keyspace(), topContentPagesTable, mapToRow(TopResult.class))
			.saveToCassandra();
	}
	
}
