package br.edu.ufcg.analytics.wikitrends.processing.batch;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaRDD;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.batchview.types.KeyValuePairPerHour;
import br.edu.ufcg.analytics.wikitrends.storage.master.types.EditChange;

public class MetricsPartialBatchJob extends AbstractPartialBatchJob {

	private static final long serialVersionUID = 4394268380743075556L;

	public MetricsPartialBatchJob(Configuration configuration) {
		super(configuration, BatchViewID.PARTIAL_METRICS);
	}
	
	@Override
	public void process(LocalDateTime currentTime) {
		JavaRDD<EditChange> wikipediaEdits = read(currentTime);
		
		long allEdits = wikipediaEdits.count();
		
		long minorEdits = wikipediaEdits.filter(edit -> edit.getMinor()).count();
		
		long sumOfAllEditLengths = wikipediaEdits.map( edit -> {
			Map<String, Long> length = edit.getLength();
			return length.getOrDefault("new", 0L) - length.getOrDefault("old", 0L);
		}).fold(0L, (a, b) -> a+b);
		
		int year = currentTime.getYear();
		int month = currentTime.getMonthValue();
		int day = currentTime.getDayOfMonth();
		int hour = currentTime.getHour();
		
		JavaRDD<KeyValuePairPerHour> distinctRDD = getJavaSparkContext().parallelize(Arrays.asList(
				new KeyValuePairPerHour(year, month, day, hour, "all_edits", allEdits),
				new KeyValuePairPerHour(year, month, day, hour, "minor_edits", minorEdits),
				new KeyValuePairPerHour(year, month, day, hour, "sum_length", sumOfAllEditLengths)
				));
		
		CassandraJavaUtil.javaFunctions(distinctRDD)
		.writerBuilder(getKeyspace(), getBatchViewID().toString(), mapToRow(KeyValuePairPerHour.class))
		.saveToCassandra();
	}
	
	private JavaRDD<EditChange> read(LocalDateTime currentTime) {
		
		JavaRDD<EditChange> wikipediaEdits = CassandraJavaUtil.javaFunctions(getJavaSparkContext()).cassandraTable("master_dataset", "edits")
				.select("server_name", "minor", "length")
				.where("year = ? and month = ? and day = ? and hour = ?", currentTime.getYear(), currentTime.getMonthValue(), currentTime.getDayOfMonth(), currentTime.getHour())
				.map( row -> {
					EditChange edit = new EditChange();
					edit.setServerName(row.getString("server_name"));
					edit.setMinor(row.getBoolean("minor"));
					edit.setLength(row.getMap("length", CassandraJavaUtil.typeConverter(String.class), CassandraJavaUtil.typeConverter(Long.class)));
					return edit;
				})
				.filter(edit -> edit.getServerName().endsWith("wikipedia.org"));
		return wikipediaEdits;
	}
}

