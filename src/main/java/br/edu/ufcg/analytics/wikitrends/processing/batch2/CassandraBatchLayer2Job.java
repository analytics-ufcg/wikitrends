package br.edu.ufcg.analytics.wikitrends.processing.batch2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.configuration.Configuration;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.ResultAbsoluteValuesShot;
import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.ResultTopContentPage;
import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.ResultTopEditor;
import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.ResultTopIdiom;
import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.ResultTopPage;

public class CassandraBatchLayer2Job extends BatchLayer2Job {

	/**
	 * SerialVersionUID for CassandraBatchLayer2Job
	 * 
	 *  @since November 26, 2015
	 */
	private static final long serialVersionUID = 1670806497796464093L;
	
	protected String servingKeyspace;
	private String topPagesTable;
	private String topContentPagesTable;
	private String topIdiomsTable;
	private String topEditorsTable;
	private String absoluteValuesTable;

	//protected String serversRankingTable;

	/**
	 * Default constructor
	 * 
	 * @param configuration
	 */
	public CassandraBatchLayer2Job(Configuration configuration) {
		super(configuration);
		servingKeyspace = configuration.getString("wikitrends.serving.cassandra.keyspace");
		topPagesTable = configuration.getString("wikitrends.serving.cassandra.table.toppages");
		topContentPagesTable = configuration.getString("wikitrends.serving.cassandra.table.topcontentpages");
		topIdiomsTable = configuration.getString("wikitrends.serving.cassandra.table.topidioms");
		topEditorsTable = configuration.getString("wikitrends.serving.cassandra.table.topeditors");
		absoluteValuesTable = configuration.getString("wikitrends.serving.cassandra.table.absolutevalues");

		//r = configuration.getString("wikitrends.serving.cassandra.table.serversranking");
	}

	@Override
	protected void saveResultTopPages(Map<String, Integer> mapTitleToCount) {
		
		List<ResultTopPage> rtpList = new ArrayList<ResultTopPage>();
		for(Entry<String, Integer> entry : mapTitleToCount.entrySet()) {
			rtpList.add(new ResultTopPage(entry.getKey(), entry.getValue()));
		}
		
		CassandraJavaUtil.javaFunctions(sc.parallelize(rtpList))
			.writerBuilder(servingKeyspace, topPagesTable, mapToRow(ResultTopPage.class))
			.saveToCassandra();
		
	}

	@Override
	protected void saveResultTopIdioms(Map<String, Integer> mapIdiomToCount) {
		
		List<ResultTopIdiom> rtpList = new ArrayList<ResultTopIdiom>();
		for(Entry<String, Integer> entry : mapIdiomToCount.entrySet()) {
			rtpList.add(new ResultTopIdiom(entry.getKey(), entry.getValue()));
		}
		
		CassandraJavaUtil.javaFunctions(sc.parallelize(rtpList))
			.writerBuilder(servingKeyspace, topIdiomsTable, mapToRow(ResultTopIdiom.class))
			.saveToCassandra();
		
	}
	
	@Override
	protected void saveResultTopContentPages(Map<String, Integer> mapContentPageToCount) {
		
		List<ResultTopContentPage> rtpList = new ArrayList<ResultTopContentPage>();
		for(Entry<String, Integer> entry : mapContentPageToCount.entrySet()) {
			rtpList.add(new ResultTopContentPage(entry.getKey(), entry.getValue()));
		}
		
		CassandraJavaUtil.javaFunctions(sc.parallelize(rtpList))
			.writerBuilder(servingKeyspace, topContentPagesTable, mapToRow(ResultTopContentPage.class))
			.saveToCassandra();
		
	}
	
	@Override
	protected void saveResultTopEditors(Map<String, Integer> mapEditorToCount) {
		
		List<ResultTopEditor> rtpList = new ArrayList<ResultTopEditor>();
		for(Entry<String, Integer> entry : mapEditorToCount.entrySet()) {
			rtpList.add(new ResultTopEditor(entry.getKey(), entry.getValue()));
		}
		
		CassandraJavaUtil.javaFunctions(sc.parallelize(rtpList))
			.writerBuilder(servingKeyspace, topEditorsTable, mapToRow(ResultTopEditor.class))
			.saveToCassandra();
		
	}
	
	@Override
	protected void saveResultAbsoluteValues(ResultAbsoluteValuesShot resultAbsValuesShot) {
		List<ResultAbsoluteValuesShot> list = Arrays.asList(resultAbsValuesShot);
		CassandraJavaUtil.javaFunctions(sc.parallelize(list))
								.writerBuilder(servingKeyspace, absoluteValuesTable, mapToRow(ResultAbsoluteValuesShot.class))
								.saveToCassandra();
	}

	/**
	 * Compute topEditors, topContentPages, topIdioms and topPages based
	 * on the hourly generated data.
	 * @param <T>
	 * 
	 * @param tableName: top_editors, top_content_pages, top_pages or top_idioms
	 * 
	 * @return map with the elements and respective amount of each one 
	 */
	private Map<String, Long> computeTopClassLongTypeData(String tableName) {
    	Map<String, Long> map = new HashMap<String, Long>();
    	
    	CassandraConnector connector = CassandraConnector.apply(sc.getConf());
    	try (Session session = connector.openSession()) {
            ResultSet results = session.execute("SELECT data FROM batch_views." + tableName);
            
            //System.out.println(results.toString());
            
            for(Row r : results) {
            	Map<String, Long> tmpM = r.getMap("data", String.class, Long.class);
            	for (String key : tmpM.keySet()) {
            	    if(map.keySet().contains(key)) {
            	    	map.put(key, map.get(key) + tmpM.get(key));
            	    }
            	    else {
            	    	map.put(key, tmpM.get(key));
            	    }
            	}
            }
    	}
        
        //System.out.println("Final map: " + map.toString());
        return map;
    }
	
	/**
	 * Compute topEditors, topContentPages, topIdioms and topPages based
	 * on the hourly generated data.
	 * @param <T>
	 * 
	 * @param tableName: top_editors, top_content_pages, top_pages or top_idioms
	 * 
	 * @return map with the elements and respective amount of each one 
	 */
	private Map<String, Integer> computeTopClassIntegerTypeData(String tableName) {
    	Map<String, Integer> map = new HashMap<String, Integer>();
    	
    	CassandraConnector connector = CassandraConnector.apply(sc.getConf());
    	try (Session session = connector.openSession()) {
            ResultSet results = session.execute("SELECT data FROM batch_views." + tableName);
            
            //System.out.println(results.toString());
            
            for(Row r : results) {
            	Map<String, Integer> tmpM = r.getMap("data", String.class, Integer.class);
            	for (String key : tmpM.keySet()) {
            	    if(map.keySet().contains(key)) {
            	    	map.put(key, map.get(key) + tmpM.get(key));
            	    }
            	    else {
            	    	map.put(key, tmpM.get(key));
            	    }
            	}
            }
    	}
        
        //System.out.println("Final map: " + map.toString());
        return map;
    }
	
	
	/**
	 * Compute the number of major edits, minor edits and average_size.
	 * @param sc 
	 * 
	 * @return map with the name 'major_edits', 'minor_edits' and 'average_size' and the respective values.
	 */
	private Map<String, Long> computeEditsData() {
    	Map<String, Long> map = new HashMap<String, Long>();
    	
    	CassandraConnector connector = CassandraConnector.apply(sc.getConf());
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
		
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());
    	try (Session session = connector.openSession()) {
            ResultSet results = session.execute("SELECT distinct_editors_set FROM batch_views." + "absolute_values");
            
            for(Row r : results) {
            	distinctEditorsList.addAll(r.getSet("distinct_editors_set", String.class));
            }
    	}
		
		return (int) sc.parallelize(distinctEditorsList).distinct().count();
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
		
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());
    	try (Session session = connector.openSession()) {
            ResultSet results = session.execute("SELECT distinct_pages_set FROM batch_views." + "absolute_values");

            for(Row r : results) {
            	distinctPagesList.addAll(r.getSet("distinct_pages_set", String.class));
            }
    	}
		
		return sc.parallelize(distinctPagesList).distinct().count();
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
		
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());
    	try (Session session = connector.openSession()) {
            ResultSet results = session.execute("SELECT distinct_servers_set FROM batch_views." + "absolute_values");
            
            for(Row r : results) {
            	distinctServersList.addAll(r.getSet("distinct_servers_set", String.class));
            }
    	}
		
		return (int) sc.parallelize(distinctServersList).distinct().count();
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
		
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());
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
	protected ResultAbsoluteValuesShot computeResultAbsoluteValues() {
		Map<String, Long> edits_data = computeEditsData();

		Long distinct_pages_count = computeDistinctPagesCount();
		Integer distincts_editors_count = computeDistinctEditorsCount();
		Integer distincts_servers_count = computeDistinctServersCount();
		
//		System.out.println(distincts_pages_set.size()); // 359185
//		System.out.println(distincts_editors_set.size()); // 57978
//		System.out.println(distincts_servers_set.size()); // 215

		Long smaller_origin = getSmallerOrigin(); 
		
		return new ResultAbsoluteValuesShot(edits_data.get("all_edits"),
											edits_data.get("minor_edits"),
											edits_data.get("average_size"),
											distinct_pages_count,
											distincts_editors_count,
											distincts_servers_count,
											smaller_origin);
	}

	@Override
	protected Map<String, Integer> computeMapEditorToCount() {
		return computeTopClassIntegerTypeData("top_editors");
	}

	@Override
	protected Map<String, Integer> computeMapPagesToCount() {
		return computeTopClassIntegerTypeData("top_pages");
	}

	@Override
	protected Map<String, Integer> computeMapIdiomsToCount() {
		return computeTopClassIntegerTypeData("top_idioms");
	}

	@Override
	protected Map<String, Integer> computeMapContentPagesToCount() {
		return computeTopClassIntegerTypeData("top_content_pages");
	}
	
}
