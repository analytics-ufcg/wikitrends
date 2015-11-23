package br.edu.ufcg.analytics.wikitrends.view.api.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;

import br.edu.ufcg.analytics.wikitrends.view.api.controller.beans.RankingRow;

/**
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 * @author Guilherme Gadelha
 * 
 */
@RestController
public class CassandraDBWikiTrendsController implements WikiTrendsController {
	
	private JavaSparkContext sc;

	public CassandraDBWikiTrendsController(JavaSparkContext sc) {
		super(); 
		this.sc = sc;
	}
	
	
	/* (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.api.controller.WikiController#statistics()
	 */
	@Override
	@RequestMapping("/statistics")
	public RankingRow[] statistics() {
		return queryAbsolute(Integer.MAX_VALUE);
	}

	/* (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.api.controller.WikiController#editors(java.lang.String)
	 */
	@Override
	@RequestMapping("/editors")
	public RankingRow[] editors(@RequestParam(value="size", defaultValue="20") String size) {
		String source = "top_editors";
		int numberOfResults = Integer.valueOf(size);
		
		return query(source, numberOfResults);
	}

	/* (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.api.controller.WikiController#idioms(java.lang.String)
	 */
	@Override
	@RequestMapping("/idioms")
	public RankingRow[] idioms(@RequestParam(value="size", defaultValue="20") String size) {
		String source = "top_idioms";
		int numberOfResults = Integer.valueOf(size);
		
		return query(source, numberOfResults);
	}

	/* (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.api.controller.WikiController#pages(java.lang.String)
	 */
	@Override
	@RequestMapping("/pages")
	public RankingRow[] pages(@RequestParam(value="size", defaultValue="20") String size, @RequestParam(value="contentonly", defaultValue="false") String contentOnly) {
		int numberOfResults = Integer.valueOf(size);
		boolean contentOnlyPages = Boolean.valueOf(contentOnly);
		String source = contentOnlyPages ? "top_pages": "top_content_pages";
		
		return query(source, numberOfResults);
	}
	
	private RankingRow[] query(String source, int numberOfResults) {
		List<RankingRow> results = new ArrayList<>();
		Map<String, Long> mapComputed = computeTopClasses(source);
		
		for(Entry<String, Long> s : mapComputed.entrySet()) {
			results.add(new RankingRow(s.getKey(), s.getValue().toString()));
		}
		
		return results.toArray(new RankingRow[results.size()]);
	}
	
	private RankingRow[] queryAbsolute(int numberOfResults) {
		List<RankingRow> results = new ArrayList<>();
		
		Map<String, Long> mapComputed = computeEditsData();
		for(Entry<String, Long> s : mapComputed.entrySet()) {
			results.add(new RankingRow(s.getKey(), s.getValue().toString()));
		}
		
		results.add(new RankingRow("distinct_editors", computeDistinctEditorsCount().toString()));
		results.add(new RankingRow("distinct_pages", computeDistinctPagesCount().toString()));
		results.add(new RankingRow("distinct_servers", computeDistinctServersCount().toString()));
		results.add(new RankingRow("origin", computeSmallerOrigin().toString()));
		
		return results.toArray(new RankingRow[results.size()]);
	}
	
	
	/**
	 * Compute the initial time since the application is active receiving data
	 * from Wikipedia.
	 * 
	 * @return initial time in milliseconds
	 */
	private Long computeSmallerOrigin() {
		
		Long smallTime = Long.MAX_VALUE;
		
		CassandraConnector connector = CassandraConnector.apply(this.sc.getConf());
    	try (Session session = connector.openSession()) {
            ResultSet results = session.execute("SELECT smaller_data FROM batch_views." + "absolute_values");
            
            for(Row r : results) {
            	if(r.getLong("smaller_origin") < smallTime) {
            		smallTime = r.getLong("smaller_origin");
            	}
            }
    	}
    	
		return smallTime;
	}

	
	/**
	 * Compute the number of distinct servers in the entire master dataset
	 * using the hourly generated data.
	 * 
	 * @return number of different servers
	 */
	private Long computeDistinctServersCount() {
		List<String> distinctServersList = new ArrayList<String>();
		
		CassandraConnector connector = CassandraConnector.apply(this.sc.getConf());
    	try (Session session = connector.openSession()) {
            ResultSet results = session.execute("SELECT distinct_servers_set FROM batch_views." + "absolute_values");
            
            for(Row r : results) {
            	distinctServersList.addAll(r.getSet("distinct_servers_set", String.class));
            }
    	}
		
		return sc.parallelize(distinctServersList).distinct().count();
	}

	/**
	 * Compute the number of distinct pages in the entire master dataset
	 * using the hourly generated data.
	 * 
	 * @return number of different pages
	 */
	private Long computeDistinctPagesCount() {
		List<String> distinctPagesList = new ArrayList<String>();
		
		CassandraConnector connector = CassandraConnector.apply(this.sc.getConf());
    	try (Session session = connector.openSession()) {
            ResultSet results = session.execute("SELECT distinct_pages_set FROM batch_views." + "absolute_values");
            
            for(Row r : results) {
            	distinctPagesList.addAll(r.getSet("distinct_pages_set", String.class));
            }
    	}
		
		return sc.parallelize(distinctPagesList).distinct().count();
	}

	/**
	 * Compute the number of distinct editors in the entire master dataset
	 * using the hourly generated data.
	 * 
	 * @return number of different editors
	 */
	private Long computeDistinctEditorsCount() {
		List<String> distinctEditorsList = new ArrayList<String>();
		
		CassandraConnector connector = CassandraConnector.apply(this.sc.getConf());
    	try (Session session = connector.openSession()) {
            ResultSet results = session.execute("SELECT distinct_editors_set FROM batch_views." + "absolute_values");
            
            for(Row r : results) {
            	distinctEditorsList.addAll(r.getSet("distinct_editors_set", String.class));
            }
    	}
		
		return sc.parallelize(distinctEditorsList).distinct().count();
	}


	/**
	 * Compute topEditors, topContentPages, topIdioms and topPages based
	 * on the hourly generated data.
	 * 
	 * @param tableName: top_editors, top_content_pages, top_pages or top_idioms
	 * 
	 * @return 
	 */
	private Map<String, Long> computeTopClasses(String tableName) {
    	Map<String, Long> map = new HashMap<String, Long>();
    	
    	CassandraConnector connector = CassandraConnector.apply(this.sc.getConf());
    	try (Session session = connector.openSession()) {
            ResultSet results = session.execute("SELECT data FROM batch_views." + tableName);
            
            System.out.println(results.toString());
            
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
        
        System.out.println("Final map: " + map.toString());
        return map;
    }
	
	/**
	 * Compute the number of major edits and minor edits.
	 * 
	 * @return map with the name 'major_edits' or 'minor_edits' and the respective values.
	 */
	private Map<String, Long> computeEditsData() {
    	Map<String, Long> map = new HashMap<String, Long>();
    	
    	CassandraConnector connector = CassandraConnector.apply(this.sc.getConf());
    	try (Session session = connector.openSession()) {
            ResultSet results = session.execute("SELECT edits_data FROM batch_views." + "absolute_values");
            
            System.out.println(results.toString());
            
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
    	}
        
        System.out.println("Final map: " + map.toString());
        return map;
    }
	
}


