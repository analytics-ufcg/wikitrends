package br.edu.ufcg.analytics.wikitrends.processing.batch2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.configuration.Configuration;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.ResultTopContentPage;

public class TopContentPagesBatch2 extends BatchLayer2Job_2 {
	
	private static final long serialVersionUID = 5181147901979329455L;
	
	private String topContentPagesTable;
	
	public TopContentPagesBatch2(Configuration configuration)  {
		super(configuration);
		
		topContentPagesTable = configuration.getString("wikitrends.serving.cassandra.table.topcontentpage");
	}
	
	@Override
	public void process() {
		Map<String, Integer> mapContentPageToCount = computeFullRankingFromPartial("top_content_pages");
		
		List<ResultTopContentPage> rtpList = new ArrayList<ResultTopContentPage>();
		for(Entry<String, Integer> entry : mapContentPageToCount.entrySet()) {
			rtpList.add(new ResultTopContentPage(entry.getKey(), entry.getValue()));
		}
		
		CassandraJavaUtil.javaFunctions(getJavaSparkContext().parallelize(rtpList))
			.writerBuilder(getBatchViews2Keyspace(), topContentPagesTable, mapToRow(ResultTopContentPage.class))
			.saveToCassandra();
		
	}
	
}
