package br.edu.ufcg.analytics.wikitrends.processing.batch2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import org.apache.commons.configuration.Configuration;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.TopResult;

public class TopEditorsBatch2 extends BatchLayer2Job_2 {
	
	private static final long serialVersionUID = -307773374341420488L;

	private String topEditorsTable;
	
	public TopEditorsBatch2(Configuration configuration) {
		super(configuration);
		
		topEditorsTable = configuration.getString("wikitrends.serving.cassandra.table.topeditor");
	}
	
	@Override
	public void process() {
		CassandraJavaUtil.javaFunctions(computeFullRankingFromPartial("top_editors"))
			.writerBuilder(getBatchViews2Keyspace(), topEditorsTable, mapToRow(TopResult.class))
			.saveToCassandra();
		
		finalizeSparkContext();
	}
}
