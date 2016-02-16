package br.edu.ufcg.analytics.wikitrends.processing.batch1;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaRDD;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.processing.JobStatusID;
import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditChange;
import br.edu.ufcg.analytics.wikitrends.storage.serving1.types.KeyValuePairPerHour;

public class AbsoluteValuesBatch1 extends BatchLayer1Job {

	private static final long serialVersionUID = 4394268380743075556L;

	private static final JobStatusID ABSOLUTE_VALUES_STATUS_ID = JobStatusID.ABS_VALUES_BATCH_1;

	private String absoluteValuesTable;

	public AbsoluteValuesBatch1(Configuration configuration) {
		super(configuration, ABSOLUTE_VALUES_STATUS_ID);
		
		absoluteValuesTable = configuration.getString("wikitrends.serving1.cassandra.table.absolutevalues");
	}
	
	@Override
	public JavaRDD<EditChange> read() {
		
		JavaRDD<EditChange> wikipediaEdits = CassandraJavaUtil.javaFunctions(getJavaSparkContext()).cassandraTable("master_dataset", "edits")
				.select("server_name", "minor", "length")
				.where("year = ? and month = ? and day = ? and hour = ?", getCurrentTime().getYear(), getCurrentTime().getMonthValue(), getCurrentTime().getDayOfMonth(), getCurrentTime().getHour())
				.map( row -> {
					EditChange edit = new EditChange();
					edit.setServerName(row.getString("server_name"));
					edit.setMinor(row.getBoolean("minor"));
					edit.setLength(row.getMap("length", CassandraJavaUtil.typeConverter(String.class), CassandraJavaUtil.typeConverter(Long.class)));
					return edit;
				});
		return wikipediaEdits;
	}

	
	@Override
	public void process() {
		JavaRDD<EditChange> wikipediaEdits = read()
				.filter(edit -> edit.getServerName().endsWith("wikipedia.org"))
				.cache();
		
		long allEdits = wikipediaEdits.count();
		
		long minorEdits = wikipediaEdits.filter(edit -> edit.getMinor()).count();
		
		long sumOfAllEditLengths = wikipediaEdits.map( edit -> {
			Map<String, Long> length = edit.getLength();
			return length.getOrDefault("new", 0L) - length.getOrDefault("old", 0L);
		}).fold(0L, (a, b) -> a+b);
		
		int year = getCurrentTime().getYear();
		int month = getCurrentTime().getMonthValue();
		int day = getCurrentTime().getDayOfMonth();
		int hour = getCurrentTime().getHour();
		
		JavaRDD<KeyValuePairPerHour> distinctRDD = getJavaSparkContext().parallelize(Arrays.asList(
				new KeyValuePairPerHour(year, month, day, hour, "all_edits", allEdits),
				new KeyValuePairPerHour(year, month, day, hour, "minor_edits", minorEdits),
				new KeyValuePairPerHour(year, month, day, hour, "sum_length", sumOfAllEditLengths)
				));
		
		CassandraJavaUtil.javaFunctions(distinctRDD)
		.writerBuilder(getBatchViews1Keyspace(), absoluteValuesTable, mapToRow(KeyValuePairPerHour.class))
		.saveToCassandra();
	}

}

