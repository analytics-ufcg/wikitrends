package br.edu.ufcg.analytics.wikitrends.processing.batch1;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.time.LocalDateTime;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.processing.JobStatusID;
import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditChange;
import br.edu.ufcg.analytics.wikitrends.storage.serving1.types.TopClass;
import scala.Tuple2;

public class TopEditorsBatch1 extends BatchLayer1Job {

	private static final JobStatusID TOP_EDITORS_STATUS_ID = JobStatusID.TOP_EDITORS_BATCH_1;
	/**
	 * @since December 3, 2015
	 */
	private static final long serialVersionUID = 1367256477428803167L;
	private String usersTable;

	public TopEditorsBatch1(Configuration configuration) {
		super(configuration, TOP_EDITORS_STATUS_ID);
		this.usersTable = configuration.getString("wikitrends.serving1.cassandra.table.editors");
	}
	
	@Override
	public JavaRDD<EditChange> read() {
		
		LocalDateTime currentTime = getCurrentTime();
		
		JavaRDD<EditChange> wikipediaEdits = javaFunctions(getJavaSparkContext()).cassandraTable("master_dataset", "edits")
				.select("bot", "server_name", "user")
				.where("year = ? and month = ? and day = ? and hour = ?", currentTime.getYear(), currentTime.getMonthValue(), currentTime.getDayOfMonth(), currentTime.getHour())
				.map(row -> {
					EditChange edit = new EditChange();
					edit.setBot(row.getBoolean("bot"));
					edit.setServerName(row.getString("server_name"));
					edit.setUser(row.getString("user"));
					return edit;
				});
		return wikipediaEdits;
	}
	
	@Override
	public void process() {
		JavaRDD<EditChange> wikipediaEdits = read()
				.filter(edit -> edit.getServerName().endsWith("wikipedia.org"))
//				.filter(edit -> !edit.getBot())
				.cache();

		JavaPairRDD<String, Integer> userRDD = wikipediaEdits
				.mapToPair( edit -> new Tuple2<String, Integer>(edit.getUser(), 1) );
		
		JavaRDD<TopClass> userRanking = transformToTopEntry(userRDD);
		
		CassandraJavaUtil.javaFunctions(userRanking)
			.writerBuilder(getBatchViews1Keyspace(), usersTable, mapToRow(TopClass.class))
			.saveToCassandra();
		
	}
}
