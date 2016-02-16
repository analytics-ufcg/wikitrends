package br.edu.ufcg.analytics.wikitrends.processing.batch;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.time.LocalDateTime;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaRDD;

import br.edu.ufcg.analytics.wikitrends.storage.master.types.EditChange;

public class EditorsPartialRankingBatchJob extends AbstractPartialRankingBatchJob {

	/**
	 * @since December 3, 2015
	 */
	private static final long serialVersionUID = 1367256477428803167L;

	public EditorsPartialRankingBatchJob(Configuration configuration) {
		super(configuration, BatchViewID.EDITORS_PARTIAL_RANKINGS);
	}

	@Override
	public JavaRDD<EditChange> read(LocalDateTime currentTime) {

		JavaRDD<EditChange> wikipediaEdits = javaFunctions(getJavaSparkContext())
				.cassandraTable("master_dataset", "edits").select("bot", "server_name", "user")
				.where("year = ? and month = ? and day = ? and hour = ?", currentTime.getYear(),
						currentTime.getMonthValue(), currentTime.getDayOfMonth(), currentTime.getHour())
				.map(row -> {
					EditChange edit = new EditChange();
					edit.setBot(row.getBoolean("bot"));
					edit.setServerName(row.getString("server_name"));
					edit.setUser(row.getString("user"));
					return edit;
				}).filter(edit -> edit.getServerName().endsWith("wikipedia.org"))
				// .filter(edit -> !edit.getBot())
				;
		return wikipediaEdits;
	}

	@Override
	protected String getRankingKey(EditChange edit) {
		return edit.getUser();
	}
}
