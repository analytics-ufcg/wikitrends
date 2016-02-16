package br.edu.ufcg.analytics.wikitrends.processing.batch;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.time.LocalDateTime;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaRDD;

import br.edu.ufcg.analytics.wikitrends.storage.master.types.EditChange;

public class IdiomsPartialRankingBatchJob extends AbstractPartialRankingBatchJob {

	private static final long serialVersionUID = -1738945554412789213L;

	public IdiomsPartialRankingBatchJob(Configuration configuration) {
		super(configuration, BatchViewID.IDIOMS_PARTIAL_RANKINGS);
	}

	@Override
	public JavaRDD<EditChange> read(LocalDateTime currentTime) {

		JavaRDD<EditChange> wikipediaEdits = javaFunctions(getJavaSparkContext())
				.cassandraTable("master_dataset", "edits").select("server_name")
				.where("year = ? and month = ? and day = ? and hour = ?", currentTime.getYear(),
						currentTime.getMonthValue(), currentTime.getDayOfMonth(), currentTime.getHour())
				.map(row -> {
					EditChange edit = new EditChange();
					edit.setServerName(row.getString("server_name"));
					return edit;
				}).filter(edit -> getRankingKey(edit).endsWith("wikipedia.org"));
		return wikipediaEdits;
	}

	@Override
	protected String getRankingKey(EditChange edit) {
		return edit.getServerName();
	}

}
