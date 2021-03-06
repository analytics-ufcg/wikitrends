package org.wikitrends.serving.datagenerator;

import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;

public class TablesGenerator {
	protected JavaSparkContext sc;
	
	public TablesGenerator(JavaSparkContext sc2) {
		this.sc = sc2;
	}
	
	public void generateTables() {
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());

        // Prepare the schema
        try (Session session = connector.openSession()) {
            session.execute("DROP KEYSPACE IF EXISTS batch_views");
            
            session.execute("CREATE KEYSPACE batch_views WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            
            session.execute("CREATE TABLE IF NOT EXISTS batch_views." +
								"top_editors" +
							    "(id INT," +
							    "date_event TIMESTAMP," +
								"hour INT," +
								"event_time TIMESTAMP," +
								"data MAP<TEXT,INT>," +
								
			    				"PRIMARY KEY((date_event, hour), event_time)," +
			    				") WITH CLUSTERING ORDER BY (event_time DESC);"
            		);
            
            session.execute("CREATE TABLE IF NOT EXISTS batch_views." +
								"top_idioms" +
							    "(id INT," +
							    "date_event TIMESTAMP," +
								"hour INT," +
								"event_time TIMESTAMP," +
								"data MAP<TEXT,INT>," +
								
			    				"PRIMARY KEY((date_event, hour), event_time)," +
			    				") WITH CLUSTERING ORDER BY (event_time DESC);"
            		);
           
            session.execute("CREATE TABLE IF NOT EXISTS batch_views." +
								"top_pages" +
							    "(id INT," +
							    "date_event TIMESTAMP," +
								"hour INT," +
								"event_time TIMESTAMP," +
								"data MAP<TEXT,INT>," +
								
			    				"PRIMARY KEY((date_event, hour), event_time)," +
			    				") WITH CLUSTERING ORDER BY (event_time DESC);"
            		);
            
            session.execute("CREATE TABLE IF NOT EXISTS batch_views." +
								"top_content_pages" +
							    "(id INT," +
							    "date_event TIMESTAMP," +
								"hour INT," +
								"event_time TIMESTAMP," +
								"data MAP<TEXT,INT>," +
								
			    				"PRIMARY KEY((date_event, hour), event_time)," +
			    				") WITH CLUSTERING ORDER BY (event_time DESC);"
            		);
            
            session.execute("CREATE TABLE IF NOT EXISTS batch_views.absolute_values (" +
							    "id INT," +
							    "date TEXT," +
							    "hour TEXT," +
								"all_edits INT," +
								"minor_edits INT," +
								"average_size INT," +
								"distinct_pages INT," +
								"distinct_servers INT," +
								"distinct_editors INT," +
								"origin BIGINT," +
								"batch_elapsed_time BIGINT," +
								"total_executor_cores INT," +
								"input_size BIGINT," +
								"event_time TIMESTAMP," +
								
								"PRIMARY KEY((id, date), event_time)," +
								") WITH CLUSTERING ORDER BY (event_time DESC);"
            		);
        }
	}
}
