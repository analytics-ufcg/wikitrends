package br.edu.ufcg.analytics.wikitrends.processing.batch;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.time.LocalDateTime;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaRDD;

import br.edu.ufcg.analytics.wikitrends.storage.master.types.EditChange;

public class PagesPartialRankingBatchJob extends AbstractPartialRankingBatchJob {

	private static final long serialVersionUID = 8312361071938353760L;

	public PagesPartialRankingBatchJob(Configuration configuration) {
		this(configuration, BatchViewID.PAGES_PARTIAL_RANKINGS);
	}

	public PagesPartialRankingBatchJob(Configuration configuration, BatchViewID statusID) {
		super(configuration, statusID);
	}

	@Override
	public JavaRDD<EditChange> read(LocalDateTime currentTime) {

		JavaRDD<EditChange> wikipediaEdits = javaFunctions(getJavaSparkContext())
				.cassandraTable("master_dataset", "edits").select("server_name", "title", "namespace")
				.where("year = ? and month = ? and day = ? and hour = ?", currentTime.getYear(),
						currentTime.getMonthValue(), currentTime.getDayOfMonth(), currentTime.getHour())
				.map(row -> {
					EditChange edit = new EditChange();
					edit.setServerName(row.getString("server_name"));
					edit.setTitle(row.getString("title"));
					edit.setNamespace(row.getInt("namespace"));
					return edit;
				}).filter(edit -> edit.getServerName().endsWith("wikipedia.org"));
		return wikipediaEdits;
	}

	@Override
	protected String getRankingKey(EditChange edit) {
		return edit.getTitle();
	}
}
