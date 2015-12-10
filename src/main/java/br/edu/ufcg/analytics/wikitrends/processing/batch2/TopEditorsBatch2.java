package br.edu.ufcg.analytics.wikitrends.processing.batch2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.configuration.Configuration;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.ResultTopEditor;

public class TopEditorsBatch2 extends BatchLayer2Job_2 {
	
	private static final long serialVersionUID = -307773374341420488L;

	private String topEditorsTable;
	
	public TopEditorsBatch2(Configuration configuration) {
		super(configuration);
		
		topEditorsTable = configuration.getString("wikitrends.serving.cassandra.table.topeditor");
	}
	
	@Override
	public void process() {
		Map<String, Integer> mapEditorToCount = computeFullRankingFromPartial("top_editors");
		
		List<ResultTopEditor> rtpList = new ArrayList<ResultTopEditor>();
		for(Entry<String, Integer> entry : mapEditorToCount.entrySet()) {
			rtpList.add(new ResultTopEditor(entry.getKey(), entry.getValue()));
		}
		
		CassandraJavaUtil.javaFunctions(getJavaSparkContext().parallelize(rtpList))
			.writerBuilder(getBatchViews2Keyspace(), topEditorsTable, mapToRow(ResultTopEditor.class))
			.saveToCassandra();
		
	}

}
