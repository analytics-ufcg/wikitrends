package br.edu.ufcg.analytics.wikitrends.processing.batch2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.configuration.Configuration;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.ResultTopPage;

public class TopPagesBatch2 extends BatchLayer2Job_2 {
	
	private static final long serialVersionUID = -8448922796164774655L;
	
	private String topPagesTable;
	
	public TopPagesBatch2(Configuration configuration) {
		super(configuration);
		
		topPagesTable = configuration.getString("wikitrends.serving.cassandra.table.toppage");
	}
	
	@Override
	public void process() {
		Map<String, Integer> mapTitleToCount = computeFullRankingFromPartial("top_pages");
		
		List<ResultTopPage> rtpList = new ArrayList<ResultTopPage>();
		for(Entry<String, Integer> entry : mapTitleToCount.entrySet()) {
			rtpList.add(new ResultTopPage(entry.getKey(), entry.getValue()));
		}
		
		CassandraJavaUtil.javaFunctions(getJavaSparkContext().parallelize(rtpList))
			.writerBuilder(getBatchViews2Keyspace(), topPagesTable, mapToRow(ResultTopPage.class))
			.saveToCassandra();
		
	}

}
