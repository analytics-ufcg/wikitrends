package br.edu.ufcg.analytics.wikitrends.processing.batch2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import org.apache.commons.configuration.Configuration;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.TopResult;

public class TopPagesBatch2 extends BatchLayer2Job_2 {
	
	private static final long serialVersionUID = -8448922796164774655L;
	
	private String topPagesTable;
	
	public TopPagesBatch2(Configuration configuration) {
		super(configuration);
		
		topPagesTable = configuration.getString("wikitrends.serving.cassandra.table.toppage");
	}
	
	@Override
	public void process() {
		CassandraJavaUtil.javaFunctions(computeFullRankingFromPartial("top_pages"))
		.writerBuilder(getBatchViews2Keyspace(), topPagesTable, mapToRow(TopResult.class))
		.saveToCassandra();
		
		finalizeSparkContext();
		
	}

}
