package br.edu.ufcg.analytics.wikitrends.processing.batch2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import org.apache.commons.configuration.Configuration;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.processing.JobStatusID;
import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.TopResult;

public class TopEditorsBatch2 extends BatchLayer2Job {
	
	private static final long serialVersionUID = -307773374341420488L;

	private String topEditorsTable;
	private final static JobStatusID TOP_EDITORS_STATUS_ID = JobStatusID.TOP_EDITORS_BATCH_2;
	private final static ProcessResultID TOP_EDITORS_PROCESS_RESULT_ID = ProcessResultID.TOP_EDITORS;
	
	public TopEditorsBatch2(Configuration configuration) {
		super(configuration, TOP_EDITORS_STATUS_ID, TOP_EDITORS_PROCESS_RESULT_ID);
		
		topEditorsTable = configuration.getString("wikitrends.serving2.cassandra.table.topeditors");
	}
	
	@Override
	public void process() {
		truncateTable(topEditorsTable);
		
		CassandraJavaUtil.javaFunctions(computeFullRankingFromPartial("top_editors"))
			.writerBuilder(getBatchViews2Keyspace(), topEditorsTable, mapToRow(TopResult.class))
			.saveToCassandra();
	}
}
