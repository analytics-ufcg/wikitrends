package br.edu.ufcg.analytics.wikitrends.processing.batch1;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.ArrayList;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditType;
import br.edu.ufcg.analytics.wikitrends.storage.serving1.types.TopClass;
import scala.Tuple2;

public class TopEditorsBatch1 extends BatchLayer1Job {

	/**
	 * @since December 3, 2015
	 */
	private static final long serialVersionUID = 1367256477428803167L;
	private String usersTable;

	public TopEditorsBatch1(Configuration configuration) {
		super(configuration);
		
		usersTable = configuration.getString("wikitrends.batch.cassandra.table.users");
	}
	
	@Override
	public JavaRDD<EditType> read(JavaSparkContext sc) {
		JavaRDD<EditType> wikipediaEdits = javaFunctions(sc).cassandraTable("master_dataset", "edits")
				.select("common_event_bot", "common_server_name", "common_event_user", "common_event_namespace", "edit_minor")
				.where("year = ? and month = ? and day = ? and hour = ?", getNow().getYear(), getNow().getMonthValue(), getNow().getDayOfMonth(), getNow().getHour())
				.map(row -> {
					EditType edit = new EditType();
					edit.setCommon_event_bot(row.getBoolean("common_event_bot"));
					edit.setCommon_server_name(row.getString("common_server_name"));
					edit.setCommon_event_user(row.getString("common_event_user"));
					edit.setCommon_event_namespace(row.getInt("common_event_namespace"));
					edit.setEditMinor(row.getBoolean("edit_minor"));
					return edit;
				});
		return wikipediaEdits;
	}
	
	@Override
	public void process(JavaSparkContext sc) {
		JavaRDD<EditType> wikipediaEdits = read(sc)
				.filter(edit -> edit.getCommon_server_name().endsWith("wikipedia.org"))
				.cache();

		JavaPairRDD<String, Integer> userRDD = wikipediaEdits
				.mapPartitionsToPair( iterator -> {
					ArrayList<Tuple2<String, Integer>> pairs = new ArrayList<>();
					while(iterator.hasNext()){
						EditType edit = iterator.next();
						pairs.add(new Tuple2<String, Integer>(edit.getCommon_event_user(), 1));
					}
					return pairs;
				});
		
		JavaRDD<TopClass> userRanking = processRankingEntry(sc, userRDD);
		
		CassandraJavaUtil.javaFunctions(userRanking)
			.writerBuilder(getBatchViewsKeyspace(), usersTable, mapToRow(TopClass.class))
			.saveToCassandra();
	}

}
