package br.edu.ufcg.analytics.wikitrends.processing.batch2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.configuration.Configuration;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.ResultTopIdiom;

public class TopIdiomsBatch2 extends BatchLayer2Job_2 {
	
	private static final long serialVersionUID = 6811359470576431827L;

	private String topIdiomsTable;
	
	public TopIdiomsBatch2(Configuration configuration) {
		super(configuration);
		
		topIdiomsTable = configuration.getString("wikitrends.serving.cassandra.table.topidiom");
	}
	
	@Override
	public void process() {
		
		Map<String, Integer> mapIdiomToCount = computeFullRankingFromPartial("top_idioms");
		
//		JavaRDD<EditType> wikipediaEdits = javaFunctions(sc).cassandraTable("master_dataset", "edits")
//				.select("id", "year", "month", "day", "hour")
//				.limit(1L)

		
//		CassandraJavaUtil.javaFunctions(sc).cassandraTable("batch_views", "status").
		
		List<ResultTopIdiom> rtpList = new ArrayList<ResultTopIdiom>();
		for(Entry<String, Integer> entry : mapIdiomToCount.entrySet()) {
			rtpList.add(new ResultTopIdiom(entry.getKey(), entry.getValue()));
		}
		
		CassandraJavaUtil.javaFunctions(getJavaSparkContext().parallelize(rtpList))
			.writerBuilder(getBatchViews2Keyspace(), topIdiomsTable, mapToRow(ResultTopIdiom.class))
			.saveToCassandra();
		
	}

}
