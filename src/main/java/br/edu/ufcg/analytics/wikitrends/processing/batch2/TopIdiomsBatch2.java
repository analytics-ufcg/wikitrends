package br.edu.ufcg.analytics.wikitrends.processing.batch2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import org.apache.commons.configuration.Configuration;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.TopResult;

public class TopIdiomsBatch2 extends BatchLayer2Job_2 {
	
	private static final long serialVersionUID = 6811359470576431827L;

	private String topIdiomsTable;
	
	public TopIdiomsBatch2(Configuration configuration) {
		super(configuration);
		
		topIdiomsTable = configuration.getString("wikitrends.serving.cassandra.table.topidiom");
	}
	
	@Override
	public void process() {
		CassandraJavaUtil.javaFunctions(computeFullRankingFromPartial("top_idioms"))
			.writerBuilder(getBatchViews2Keyspace(), topIdiomsTable, mapToRow(TopResult.class))
			.saveToCassandra();
		
		finalizeSparkContext();
	}

}
