
package org.wikitrends.serving;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;

import br.edu.ufcg.analytics.wikitrends.storage.serving.DataGenerator;

public class TopClasses extends WikitrendsApp implements Serializable {

	private static final long serialVersionUID = -2626149675899374916L;
	
	public TopClasses(JavaSparkContext sc, String className) {
        super(sc, className);
    }

    public void run() {
        DataGenerator gen = new DataGenerator(sc, className);
        gen.generateData();
        
        compute();
        showResults();
    }

    
    @Override
    public void compute() {
    	Map<String, Integer> map = new HashMap<String, Integer>();
    	
    	CassandraConnector connector = CassandraConnector.apply(sc.getConf());
    	try (Session session = connector.openSession()) {
            ResultSet results = session.execute("SELECT data FROM batch_views." + this.className);
            
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
    }
    
    @Override
    public void showResults() {
    	CassandraConnector connector = CassandraConnector.apply(sc.getConf());
    	
    	System.out.println("Printing loaded data...");
    	try (Session session = connector.openSession()) {
            ResultSet results = session.execute("SELECT * FROM batch_views." + this.className);
            
            System.out.println(String.format("%-10s\t%-30s\t%-10s\t%-30s\t%-20s", "id", "date_event", "hour", "event_time", "data"));
        	for (Row row : results) {
        		System.out.println(String.format("%-10s\t%-30s\t%-10s\t%-30s\t%-20s", 
        													row.getInt("id"), 
        													row.getTimestamp("date_event"),
        													row.getInt("hour"),
        													row.getTimestamp("event_time"),
        													row.getMap("data", String.class, Integer.class).toString()));
        	}
            System.out.println();
        }
    }

}