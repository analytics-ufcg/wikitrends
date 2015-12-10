package br.edu.ufcg.analytics.wikitrends.processing.batch2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import org.apache.commons.configuration.Configuration;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.TopResult;

public class TopContentPagesBatch2 extends BatchLayer2Job_2 {
	
	private static final long serialVersionUID = 5181147901979329455L;
	
	private String topContentPagesTable;
	
	public TopContentPagesBatch2(Configuration configuration)  {
		super(configuration);
		
		topContentPagesTable = configuration.getString("wikitrends.serving.cassandra.table.topcontentpage");
	}
	
	@Override
	public void process() {
		CassandraJavaUtil.javaFunctions(computeFullRankingFromPartial("top_content_pages"))
			.writerBuilder(getBatchViews2Keyspace(), topContentPagesTable, mapToRow(TopResult.class))
			.saveToCassandra();
		
		finalizeSparkContext();
		
	}
	
}
