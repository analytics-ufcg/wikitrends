package br.edu.ufcg.analytics.wikitrends.storage.serving;

import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Session;

public class TablesGenerator {
	protected JavaSparkContext sc;
	private Session session;
	
	public TablesGenerator(JavaSparkContext sc2) {
		this.sc = sc2;
	}
	
	public TablesGenerator(Session session) {
		this.session = session;
	}
	
	// Prepare the schema
	public void generateTables() {
            session.execute("DROP KEYSPACE IF EXISTS batch_views");
            
            session.execute("CREATE KEYSPACE batch_views WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            
            session.execute("CREATE TABLE IF NOT EXISTS batch_views." +
								"top_editors" +
							    "(id UUID," +
								"data MAP<TEXT,INT>," +
								
								"year INT," +
								"month INT," +
								"day INT," +
								"hour INT," +
								"event_time TIMESTAMP," +
								
								"PRIMARY KEY((year, month, day, hour), id)," +
								") WITH CLUSTERING ORDER BY (id DESC);"
            		);
            
            session.execute("CREATE TABLE IF NOT EXISTS batch_views." +
								"top_idioms" +
								"(id UUID," +
								"data MAP<TEXT,INT>," +
								
								"year INT," +
								"month INT," +
								"day INT," +
								"hour INT," +
								"event_time TIMESTAMP," +
								
								"PRIMARY KEY((id), year, month, day, hour), id)," +
								") WITH CLUSTERING ORDER BY (id DESC);"
            		);
           
            session.execute("CREATE TABLE IF NOT EXISTS batch_views." +
								"top_pages" +
								"(id UUID," +
								"data MAP<TEXT,INT>," +
								
								"year INT," +
								"month INT," +
								"day INT," +
								"hour INT," +
								"event_time TIMESTAMP," +
								
								"PRIMARY KEY((id), year, month, day, hour)," +
								") WITH CLUSTERING ORDER BY (id DESC);"
            		);
            
            session.execute("CREATE TABLE IF NOT EXISTS batch_views." +
								"top_content_pages" +
								"(id UUID," +
								"data MAP<TEXT,INT>," +
								
								"year INT," +
								"month INT," +
								"day INT," +
								"hour INT," +
								"event_time TIMESTAMP," +
								
								"PRIMARY KEY((id), year, month, day, hour)," +
								") WITH CLUSTERING ORDER BY (id DESC);"
            		);
            
            session.execute("CREATE TABLE IF NOT EXISTS batch_views." +
					            "absolute_values" +
								"(id UUID," +
								"edits_data MAP<TEXT,BIGINT>," +
								
								"distincts_pages_set SET<TEXT>," +
								"distincts_editors_set SET<TEXT>," +
								"distincts_servers_set SET<TEXT>," +
								
								"smaller_origin BIGINT," +
					
								"year INT," +
								"month INT," +
								"day INT," +
								"hour INT," +
								"event_time TIMESTAMP," +
					
								"PRIMARY KEY((year, month, day, hour), id)," +
								") WITH CLUSTERING ORDER BY (id DESC);"
					);
            
	}
}
