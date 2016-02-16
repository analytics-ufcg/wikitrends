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

public class TopContentPagesBatch1 extends BatchLayer1Job {

	private static final long serialVersionUID = 5005439419731611631L;

	private static final JobStatusID TOP_CONTENT_PAGES_STATUS_ID = JobStatusID.TOP_CONTENT_PAGES_BATCH_1;

	private String contentPagesTable;

	public TopContentPagesBatch1(Configuration configuration) {
		super(configuration, TOP_CONTENT_PAGES_STATUS_ID);
		
		contentPagesTable = configuration.getString("wikitrends.serving1.cassandra.table.contentpages");
	}

	@Override
	public JavaRDD<EditChange> read() {
		
		LocalDateTime currentTime = getCurrentTime();
		
		JavaRDD<EditChange> wikipediaEdits = javaFunctions(getJavaSparkContext()).cassandraTable("master_dataset", "edits")
				.select("server_name", "title", "namespace")
				.where("year = ? and month = ? and day = ? and hour = ?", currentTime.getYear(), currentTime.getMonthValue(), currentTime.getDayOfMonth(), currentTime.getHour())
				.map(row -> {
					EditChange edit = new EditChange();
					edit.setServerName(row.getString("server_name"));
					edit.setTitle(row.getString("title"));
					edit.setNamespace(row.getInt("namespace"));
					return edit;
				});
		return wikipediaEdits;
	}

	public void process() {
		JavaRDD<EditChange> wikipediaEdits = read()
				.filter(edit -> edit.getServerName().endsWith("wikipedia.org") && edit.getNamespace() == 0)
				.cache();
		
		JavaPairRDD<String, Integer> contentTitleRDD = wikipediaEdits
				.mapToPair( edit -> new Tuple2<String, Integer>(edit.getTitle(), 1));
		
		JavaRDD<TopClass> contentTitleRanking = transformToTopEntry(contentTitleRDD);
		
		CassandraJavaUtil.javaFunctions(contentTitleRanking)
			.writerBuilder(getBatchViews1Keyspace(), contentPagesTable, mapToRow(TopClass.class))
			.saveToCassandra();
	}
}
