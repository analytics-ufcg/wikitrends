package br.edu.ufcg.analytics.wikitrends.storage.serving;

import java.io.Serializable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraServingLayerManager implements Serializable {
	
	private static final long serialVersionUID = -6109094054314874995L;

	// Prepare the schema
	public void createTables(Session session) {
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
								
								"PRIMARY KEY((year, month, day, hour), id)," +
								") WITH CLUSTERING ORDER BY (id DESC);"
            		);
           
            session.execute("CREATE TABLE IF NOT EXISTS batch_views.servers_ranking (" + 
            		"year int," +
            		"month int," +
            		"day int," +
            		"hour int," +
            		"server_name text," +
            		"number_of_access int," +
            		"PRIMARY KEY((year, month, day, hour), number_of_access)) " + 
            		"WITH CLUSTERING ORDER BY (number_of_access DESC);"
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
								
								"PRIMARY KEY((year, month, day, hour), id)," +
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
								
								"PRIMARY KEY((year, month, day, hour), id)," +
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
	
	
	
	/**
	 * Entry point
	 * 
	 * @param args
	 *            cassandra seed node address.
	 */
	public static void main(String[] args) {

		if (args.length < 2) {
			System.err.println(
					"Usage: java -cp <CLASSPATH> br.edu.ufcg.analytics.wikitrends.storage.serving.CassandraServingLayerManager OPERATION <seed_address>");
			System.exit(1);
		}

		String operation = args[0];
		String seedNode = args[1];
		
		CassandraServingLayerManager manager = new CassandraServingLayerManager();
		
		switch (operation) {
		case "CREATE":
			try (Cluster cluster = Cluster.builder().addContactPoints(seedNode).build();
					Session session = cluster.newSession();) {
				manager.createTables(session);
			}
			break;
		default:
			System.err.println("Unsupported operation. Choose CREATE as operation.");
			break;
		}

	}
}
