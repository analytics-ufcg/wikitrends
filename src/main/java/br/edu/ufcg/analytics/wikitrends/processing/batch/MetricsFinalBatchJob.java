package br.edu.ufcg.analytics.wikitrends.processing.batch2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowToTuple;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaRDD;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.processing.JobStatusID;
import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.KeyValuePair;
import scala.Tuple2;

public class AbsoluteValuesBatch2 extends BatchLayer2Job {
	
	private static final long serialVersionUID = -8968582683538025373L;
	
	private String absoluteValuesTable;

	private final static JobStatusID ABS_VALUES_STATUS_ID = JobStatusID.ABS_VALUES_BATCH_2;
	private final static ProcessResultID ABSOLUTE_VALUES_PROCESS_RESULT_ID = ProcessResultID.ABSOLUTE_VALUES;

	public AbsoluteValuesBatch2(Configuration configuration) {
		super(configuration, ABS_VALUES_STATUS_ID, ABSOLUTE_VALUES_PROCESS_RESULT_ID);
		
		absoluteValuesTable = configuration.getString("wikitrends.serving2.cassandra.table.absolutevalues");
	}
	
	@Override
	public void process() {
		JavaRDD<KeyValuePair> rdd = javaFunctions(getJavaSparkContext())
		.cassandraTable("batch_views1", absoluteValuesTable, mapRowToTuple(String.class, Long.class))
		.select("name", "value")
		.mapToPair(row -> new Tuple2<String, Long>(row._1, row._2))
		.reduceByKey((a,b) -> a+b)
		.map( tuple -> new KeyValuePair(getProcessResultID(), tuple._1, tuple._2));
		
		CassandraJavaUtil.javaFunctions(rdd)
		.writerBuilder(getBatchViews2Keyspace(), absoluteValuesTable, mapToRow(KeyValuePair.class))
		.saveToCassandra();
	}
	
}
