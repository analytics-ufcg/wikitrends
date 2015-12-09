package br.edu.ufcg.analytics.wikitrends.processing.batch1;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.ArrayList;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditChange;
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
		
		usersTable = configuration.getString("wikitrends.batch.cassandra.table.editors");
	}
	
	@Override
	public JavaRDD<EditChange> read() {
		JavaRDD<EditChange> wikipediaEdits = javaFunctions(getJavaSparkContext()).cassandraTable("master_dataset", "edits")
				.select("bot", "server_name", "user", "namespace", "minor")
				.where("year = ? and month = ? and day = ? and hour = ?", getCurrentTime().getYear(), getCurrentTime().getMonthValue(), getCurrentTime().getDayOfMonth(), getCurrentTime().getHour())
				.map(row -> {
					EditChange edit = new EditChange();
					edit.setBot(row.getBoolean("bot"));
					edit.setServerName(row.getString("server_name"));
					edit.setUser(row.getString("user"));
					edit.setNamespace(row.getInt("namespace"));
					edit.setMinor(row.getBoolean("minor"));
					return edit;
				});
		return wikipediaEdits;
	}
	
	@Override
	public void process() {
		JavaRDD<EditChange> wikipediaEdits = read()
				.filter(edit -> edit.getServerName().endsWith("wikipedia.org"))
				.cache();

		JavaPairRDD<String, Integer> userRDD = wikipediaEdits
				.mapPartitionsToPair( iterator -> {
					ArrayList<Tuple2<String, Integer>> pairs = new ArrayList<>();
					while(iterator.hasNext()){
						EditChange edit = iterator.next();
						pairs.add(new Tuple2<String, Integer>(edit.getUser(), 1));
					}
					return pairs;
				});
		
		JavaRDD<TopClass> userRanking = transformToTopEntry(userRDD);
		
		CassandraJavaUtil.javaFunctions(userRanking)
			.writerBuilder(getBatchViewsKeyspace(), usersTable, mapToRow(TopClass.class))
			.saveToCassandra();
		
		finalizeSparkContext();
	}

}
