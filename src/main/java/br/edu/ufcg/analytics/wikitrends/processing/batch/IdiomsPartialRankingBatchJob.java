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

public class TopIdiomsBatch1 extends BatchLayer1Job {

	private static final long serialVersionUID = -1738945554412789213L;
	
	private String tableName;
	
	public TopIdiomsBatch1(Configuration configuration) {
		super(configuration, JobStatusID.TOP_IDIOMS_BATCH_1);
		tableName = configuration.getString("wikitrends.serving1.cassandra.table.idioms");
	}
	
	@Override
	public JavaRDD<EditChange> read() {
		
		LocalDateTime currentTime = getCurrentTime();
		
		JavaRDD<EditChange> wikipediaEdits = javaFunctions(getJavaSparkContext()).cassandraTable("master_dataset", "edits")
				.select("server_name")
				.where("year = ? and month = ? and day = ? and hour = ?", currentTime.getYear(), currentTime.getMonthValue(), currentTime.getDayOfMonth(), currentTime.getHour())
				.map(row -> {
					EditChange edit = new EditChange();
					edit.setServerName(row.getString("server_name"));
					return edit;
				});
		return wikipediaEdits;
	}

	
	@Override
	public void process() {
		JavaRDD<EditChange> wikipediaEdits = read()
				.filter(edit -> edit.getServerName().endsWith("wikipedia.org"))
				.cache();
		
		JavaPairRDD<String, Integer> serverRDD = wikipediaEdits
			.mapToPair( edit -> new Tuple2<String, Integer>(edit.getServerName(), 1));
		
		JavaRDD<TopClass> serverRanking = transformToTopEntry(serverRDD);
		
		CassandraJavaUtil.javaFunctions(serverRanking)
			.writerBuilder(getBatchViews1Keyspace(), tableName, mapToRow(TopClass.class))
			.saveToCassandra();
	}

}
