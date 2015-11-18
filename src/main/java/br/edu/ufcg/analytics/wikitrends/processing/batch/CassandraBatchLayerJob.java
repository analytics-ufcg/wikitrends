package br.edu.ufcg.analytics.wikitrends.processing.batch;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.datastax.spark.connector.japi.CassandraRow;

import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditType;

public class CassandraBatchLayerJob extends BatchLayerJob {

	public CassandraBatchLayerJob(Configuration configuration) {
		super(configuration);
	}

	@Override
	protected JavaRDD<EditType> readRDD(JavaSparkContext sc) {
		JavaRDD<EditType> wikipediaEdits = javaFunctions(sc).cassandraTable("master_dataset", "edits")
				.select("common_event_bot", "common_event_title", "common_server_name", "common_event_user",
						"common_event_namespace", "edit_minor", "edit_length")
				.map(new Function<CassandraRow, EditType>() {
					private static final long serialVersionUID = 1L;

					@Override
					public EditType call(CassandraRow v1) throws Exception {
						EditType edit = new EditType();
						edit.setCommon_event_bot(v1.getBoolean("common_event_bot"));
						edit.setCommon_event_title(v1.getString("common_event_title"));
						edit.setCommon_event_user(v1.getString("common_event_user"));
						edit.setCommon_event_namespace(v1.getString("common_event_namespace"));
						edit.setCommon_server_name(v1.getString("common_server_name"));
						edit.setEdit_minor(v1.getBoolean("edit_minor"));
						// edit.setEdit_length(v1.getMap("edit_length"));
						return edit;
					}

				});
		return wikipediaEdits;
	}

	@Override
	protected void saveTitleRanking(JavaSparkContext sc, JavaRDD<BatchLayerOutput> titleRanking) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void saveContentTitleRanking(JavaSparkContext sc, JavaRDD<BatchLayerOutput> contentTitleRanking) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void saveServerRanking(JavaSparkContext sc, JavaRDD<BatchLayerOutput> serverRanking) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void saveUserRanking(JavaSparkContext sc, JavaRDD<BatchLayerOutput> userRanking) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void processStatistics(JavaSparkContext sc, JavaRDD<EditType> wikipediaEdits) {
		// TODO Auto-generated method stub
		
	}


}
