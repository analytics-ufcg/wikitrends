package br.edu.ufcg.analytics.wikitrends.processing.batch;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;

import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditType;
import br.edu.ufcg.analytics.wikitrends.storage.serving.types.ServerRanking;

public class CassandraIncrementalBatchLayerJob extends CassandraBatchLayerJob{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7386905244759035777L;
	private LocalDateTime start;
	private LocalDateTime now;

	/**
	 * Default constructor
	 * 
	 * @param configuration
	 */
	public CassandraIncrementalBatchLayerJob(Configuration configuration) {
		super(configuration);
		start = LocalDateTime.ofInstant(Instant.ofEpochMilli(configuration.getLong("wikitrends.batch.incremental.start") * 1000), ZoneId.systemDefault());
//		now = LocalDateTime.ofInstant(Instant.ofEpochMilli((System.currentTimeMillis() / 3600000) * 3600000), ZoneId.systemDefault());
		now = start.plusHours(3);
	}
	
	@Override
	public void run() {
		while(start.isBefore(now)){
			super.run();
			start = start.plusHours(1);
		}
		
//		summarize();
	}
	
	@Override
	protected JavaRDD<EditType> readRDD(JavaSparkContext sc) {
		System.out.println(start);
		JavaRDD<EditType> wikipediaEdits = javaFunctions(sc).cassandraTable("master_dataset", "edits")
				.select("event_time", "common_event_bot", "common_event_title", "common_server_name", "common_event_user",
						"common_event_namespace", "edit_minor", "edit_length")
				.where("year = ? and month = ? and day = ? and hour = ?", start.getYear(), start.getMonthValue(), start.getDayOfMonth(), start.getHour())
				.map(new Function<CassandraRow, EditType>() {
					private static final long serialVersionUID = 1L;

					@Override
					public EditType call(CassandraRow v1) throws Exception {
						EditType edit = new EditType();
						edit.setEvent_time(v1.getDate("event_time"));
						edit.setCommon_event_bot(v1.getBoolean("common_event_bot"));
						edit.setCommon_event_title(v1.getString("common_event_title"));
						edit.setCommon_event_user(v1.getString("common_event_user"));
						edit.setCommon_event_namespace(v1.getInt("common_event_namespace"));
						edit.setCommon_server_name(v1.getString("common_server_name"));
						edit.setEditMinor(v1.getBoolean("edit_minor"));
						edit.setEdit_length(v1.getMap("edit_length", CassandraJavaUtil.typeConverter(String.class), CassandraJavaUtil.typeConverter(Long.class)));
						return edit;
					}

				});
		System.out.println(wikipediaEdits.count());
		return wikipediaEdits;
	}
	
	@Override
	protected void saveServerRanking(JavaSparkContext sc, JavaRDD<BatchLayerOutput<Integer>> serverRanking) {
		CassandraJavaUtil
				.javaFunctions(serverRanking.map(entry -> new ServerRanking(start, entry.getKey(), entry.getValue())))
				.writerBuilder(batchViewsKeyspace, serversRankingTable, mapToRow(ServerRanking.class))
				.saveToCassandra();
	}



}
