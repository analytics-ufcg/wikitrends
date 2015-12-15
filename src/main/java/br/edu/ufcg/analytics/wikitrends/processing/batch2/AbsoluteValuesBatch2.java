package br.edu.ufcg.analytics.wikitrends.processing.batch2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

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
	
	
	/**
	 * Compute the number of major edits, minor edits and average_size.
	 * @param sc 
	 * 
	 * @return map with the name 'major_edits', 'minor_edits' and 'average_size' and the respective values.
	 */
	private Map<String, Long> computeEditsData() {
    	Map<String, Long> map = new HashMap<String, Long>();
    	
    	CassandraConnector connector = CassandraConnector.apply(getJavaSparkContext().getConf());
    	try (Session session = connector.openSession()) {
            ResultSet results = session.execute("SELECT edits_data FROM batch_views." + "absolute_values");
            
            System.out.println(results.toString());
            int amountRecords = results.all().size();
            
            for(Row r : results) {
            	Map<String, Long> tmpM = r.getMap("edits_data", String.class, Long.class);
            	for (String key : tmpM.keySet()) {
            		if(map.keySet().contains(key)) {
            			map.put(key, map.get(key) + tmpM.get(key));
            	    }
            	    else {
            	    	map.put(key, tmpM.get(key));
            	    }
            	}
            }
            map.put("average_size", map.get("average_size")/amountRecords);
    	}
        
        //System.out.println("Final map: " + map.toString());
    	
        return map;
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
            ResultSet results = session.execute("SELECT distinct_editors_set FROM batch_views." + "absolute_values");
            
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
            ResultSet results = session.execute("SELECT distinct_pages_set FROM batch_views." + "absolute_values");

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
            ResultSet results = session.execute("SELECT distinct_servers_set FROM batch_views." + "absolute_values");
            
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
            ResultSet results = session.execute("SELECT smaller_data FROM batch_views." + "absolute_values");
            
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
		Map<String, Long> edits_data = computeEditsData();

		Long distinct_pages_count = computeDistinctPagesCount();
		Integer distincts_editors_count = computeDistinctEditorsCount();
		Integer distincts_servers_count = computeDistinctServersCount();
		
		Long smaller_origin = getSmallerOrigin(); 
		
		ResultAbsoluteValuesShot result = new ResultAbsoluteValuesShot(getProcessResultID(),
																		edits_data.get("all_edits"),
																		edits_data.get("minor_edits"),
																		edits_data.get("average_size"),
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
