package br.edu.ufcg.analytics.wikitrends.processing.batch2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaRDD;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.processing.JobStatusID;
import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.ResultAbsoluteValuesShot;

public class AbsoluteValuesBatch2 extends BatchLayer2Job {
	
	private static final long serialVersionUID = -8968582683538025373L;
	
	private String absoluteValuesTable;

	private final static JobStatusID ABS_VALUES_STATUS_ID = JobStatusID.ABS_VALUES_BATCH_2;
	private final static ProcessResultID ABSOLUTE_VALUES_PROCESS_RESULT_ID = ProcessResultID.ABSOLUTE_VALUES;

	public AbsoluteValuesBatch2(Configuration configuration) {
		super(configuration, ABS_VALUES_STATUS_ID, ABSOLUTE_VALUES_PROCESS_RESULT_ID);
		
		absoluteValuesTable = configuration.getString("wikitrends.serving2.cassandra.table.absolutevalues");
	}
	
	public Long computeAllEdits() {
		Long all_edits_sum = javaFunctions(getJavaSparkContext())
			.cassandraTable("batch_views1", "absolute_values")
			.select("all_edits")
			.map(s -> s.getLong("all_edits"))
			.reduce((a,b) -> a+b);
		return all_edits_sum;
	}
	
	public Long computeMinorEdits() {
		Long minor_edits_sum = javaFunctions(getJavaSparkContext())
				.cassandraTable("batch_views1", "absolute_values")
				.select("minor_edits")
				.map(s -> s.getLong("minor_edits"))
				.reduce((a,b) -> a+b);
			return minor_edits_sum;
	}

	public Long computeAverageSize() {
		JavaRDD<Long> average_sizeRDD = javaFunctions(getJavaSparkContext())
				.cassandraTable("batch_views1", "absolute_values")
				.select("average_size")
				.map(s -> s.getLong("average_size"));
		
		Long average_size_sum = average_sizeRDD
				.reduce((a,b) -> a+b);
		
		return average_size_sum / average_sizeRDD.filter(aveSizes -> { return aveSizes.getLong("average_size") > 0L; }).count();
	}
	
	/**
	 * Compute the number of distinct editors in the entire master dataset
	 * using the hourly generated data.
	 * 
	 * @return number of different editors
	 */
	private Integer computeDistinctEditorsCount() {
		List<String> distinctEditorsList = new ArrayList<String>();
		
		CassandraConnector connector = CassandraConnector.apply(getJavaSparkContext().getConf());
    	try (Session session = connector.openSession()) {
            ResultSet results = session.execute("SELECT distinct_editors_set FROM batch_views1." + "absolute_values");
            
            for(Row r : results) {
            	distinctEditorsList.addAll(r.getSet("distinct_editors_set", String.class));
            }
    	}
		
		return (int) getJavaSparkContext().parallelize(distinctEditorsList).distinct().count();
	}


	
	/**
	 * Compute the number of distinct pages in the entire master dataset
	 * using the hourly generated data.
	 * @param sc 
	 * 
	 * @return number of different pages
	 */
	private Long computeDistinctPagesCount() {
		List<String> distinctPagesList = new ArrayList<String>();
		
		CassandraConnector connector = CassandraConnector.apply(getJavaSparkContext().getConf());
    	try (Session session = connector.openSession()) {
            ResultSet results = session.execute("SELECT distinct_pages_set FROM batch_views1." + "absolute_values");

            for(Row r : results) {
            	distinctPagesList.addAll(r.getSet("distinct_pages_set", String.class));
            }
    	}
		
		return getJavaSparkContext().parallelize(distinctPagesList).distinct().count();
	}

	
	/**
	 * Compute the number of distinct servers in the entire master dataset
	 * using the hourly generated data.
	 * @param sc 
	 * 
	 * @return number of different servers
	 */
	private Integer computeDistinctServersCount() {
		List<String> distinctServersList = new ArrayList<String>();
		
		CassandraConnector connector = CassandraConnector.apply(getJavaSparkContext().getConf());
    	try (Session session = connector.openSession()) {
            ResultSet results = session.execute("SELECT distinct_servers_set FROM batch_views1." + "absolute_values");
            
            for(Row r : results) {
            	distinctServersList.addAll(r.getSet("distinct_servers_set", String.class));
            }
    	}
		
		return (int) getJavaSparkContext().parallelize(distinctServersList).distinct().count();
	}
	
	/**
	 * Compute the initial time since the application is active receiving data
	 * from Wikipedia.
	 * @param sc 
	 * 
	 * @return initial time in milliseconds
	 */
	private Long getSmallerOrigin() {
		
		Long smallerTime = Long.MAX_VALUE;
		
		CassandraConnector connector = CassandraConnector.apply(getJavaSparkContext().getConf());
    	try (Session session = connector.openSession()) {
            ResultSet results = session.execute("SELECT smaller_origin FROM batch_views1." + "absolute_values");
            
            for(Row r : results) {
            	if(r.getLong("smaller_origin") < smallerTime) {
            		smallerTime = r.getLong("smaller_origin");
            	}
            }
    	}
    	
		return smallerTime;
	}

	@Override
	public void process() {
		truncateResultingTable(absoluteValuesTable);
		
		Long all_edits = computeAllEdits();
		Long minor_edits = computeMinorEdits();
		Long average_size = computeAverageSize();

		Long distinct_pages_count = computeDistinctPagesCount();
		Integer distincts_editors_count = computeDistinctEditorsCount();
		Integer distincts_servers_count = computeDistinctServersCount();
		
		Long smaller_origin = getSmallerOrigin(); 
		
		ResultAbsoluteValuesShot result = new ResultAbsoluteValuesShot(getProcessResultID(),
																		all_edits,
																		minor_edits,
																		average_size,
																		distinct_pages_count,
																		distincts_editors_count,
																		distincts_servers_count,
																		smaller_origin);
		
		List<ResultAbsoluteValuesShot> list = Arrays.asList(result);
		CassandraJavaUtil.javaFunctions(getJavaSparkContext().parallelize(list))
								.writerBuilder(getBatchViews2Keyspace(), absoluteValuesTable, mapToRow(ResultAbsoluteValuesShot.class))
								.saveToCassandra();
	}
	
}
