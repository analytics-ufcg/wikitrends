
package org.wikitrends.serving;

import java.io.Serializable;

import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;

import br.edu.ufcg.analytics.wikitrends.storage.serving.DataGenerator;

public class AbsoluteValues extends WikitrendsApp implements Serializable  {

	private static final long serialVersionUID = 5125150398907555238L;
	
	public AbsoluteValues(JavaSparkContext sc, String className) {
    	super(sc, className);
    }

	@Override
	public void run() {
		DataGenerator gen = new DataGenerator(this.sc, className);
        gen.generateData();
        
        compute();
        showResults();
        
        sc.stop();
	}

	@Override
	public void compute() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void showResults() {
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());
    	
		System.out.println("Printing loaded data...");
		
    	try (Session session = connector.openSession()) {
            ResultSet results = session.execute("SELECT * FROM batch_views.absolute_values");
            
            System.out.println(String.format("id, date, event_time, all_edits, average_size, batch_elapse_time, distinct_editors, distinct_pages, distinct_servers, hour, input_size, minor_edits, origin, total_executor_cores"));
        	for (Row row : results) {
        		System.out.println(row.toString());
        	}
            System.out.println();
        }
	}
}