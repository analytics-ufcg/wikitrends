package br.edu.ufcg.analytics.wikitrends.view.api.controller;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;

import br.edu.ufcg.analytics.wikitrends.view.api.controller.beans.RankingRow;

/**
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 *
 */
@RestController
public class CassandraDBWikiTrendsController implements WikiTrendsController {
	
	private JavaSparkContext sc;

	public CassandraDBWikiTrendsController(JavaSparkContext sc) {
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
		Map<String, Integer> mapComputed = computeTopClasses(source);
		
		for(Entry<String, Integer> s : mapComputed.entrySet()) {
			results.add(new RankingRow(s.getKey(), s.getValue().toString()));
		}
		
		return results.toArray(new RankingRow[results.size()]);
	}
	
	private RankingRow[] queryAbsolute(int numberOfResults) {
		List<RankingRow> results = new ArrayList<>();
		Map<String, Integer> mapComputed = computeAbsoluteValues();
		
		for(Entry<String, Integer> s : mapComputed.entrySet()) {
			results.add(new RankingRow(s.getKey(), s.getValue().toString()));
		}
		
		return results.toArray(new RankingRow[results.size()]);
	}
	
	public Map<String, Integer> computeTopClasses(String tableName) {
    	Map<String, Integer> map = new HashMap<String, Integer>();
    	
    	CassandraConnector connector = CassandraConnector.apply(this.sc.getConf());
    	try (Session session = connector.openSession()) {
            ResultSet results = session.execute("SELECT data FROM batch_views." + tableName);
            
            System.out.println(results.toString());
            
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
        
        System.out.println("Final map: " + map.toString());
        return map;
    }
	
	public Map<String, Integer> computeAbsoluteValues() {
    	Map<String, Integer> map = new HashMap<String, Integer>();
    	
    	CassandraConnector connector = CassandraConnector.apply(this.sc.getConf());
    	try (Session session = connector.openSession()) {
            ResultSet results = session.execute("SELECT data FROM batch_views." + "absolute_values");
            
            System.out.println(results.toString());
            
            for(Row r : results) {
            	Map<String, String> tmpM = r.getMap("data", String.class, String.class);
            	for (String key : tmpM.keySet()) {
            		if(map.keySet().contains(key)) {
            			if(key.equals("all_edits") || key.equals("minor_edits")) {
            				map.put(key, map.get(key) + Integer.parseInt(tmpM.get(key)) );
            			}
            			if(key.equals("minor_edits"))
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


