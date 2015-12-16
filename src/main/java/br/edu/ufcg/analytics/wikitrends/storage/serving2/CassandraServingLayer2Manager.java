package br.edu.ufcg.analytics.wikitrends.storage.serving2;

import java.io.Serializable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraServingLayer2Manager implements Serializable {
	

	/**
	 * SerialVersionUID for CassandraResultsLayerManager
	 * 
	 * @since November 25, 2015
	 */
	private static final long serialVersionUID = -1465057423342253096L;

	// Prepare the schema
	public void createTables(Session session) {
                    
            session.execute("CREATE KEYSPACE IF NOT EXISTS batch_views2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            
            session.execute("CREATE TABLE IF NOT EXISTS batch_views2.top_editors" +
								"(id TEXT," +
								"name TEXT," +
								"count BIGINT," +
								"PRIMARY KEY((id), count, name)" +
								") WITH CLUSTERING ORDER BY (count DESC, name ASC);"
            		);
            
            session.execute("CREATE TABLE IF NOT EXISTS batch_views2.top_idioms" +
								"(id TEXT," +
								"name TEXT," +
								"count BIGINT," +
								"PRIMARY KEY((id), count, name)" +
								") WITH CLUSTERING ORDER BY (count DESC, name ASC);"
            		);
           
            session.execute("CREATE TABLE IF NOT EXISTS batch_views2.top_pages" +
								"(id TEXT," +
								"name TEXT," +
								"count BIGINT," +
								"PRIMARY KEY((id), count, name)" +
								") WITH CLUSTERING ORDER BY (count DESC, name ASC);"
            		);
            
            session.execute("CREATE TABLE IF NOT EXISTS batch_views2.top_content_pages" +
								"(id TEXT," +
								"name TEXT," +
								"count BIGINT," +
								"PRIMARY KEY((id), count, name)" +
								") WITH CLUSTERING ORDER BY (count DESC, name ASC);"
					);
            
            session.execute("CREATE TABLE IF NOT EXISTS batch_views2." +
					            "absolute_values" +
								"(id TEXT," +
								"all_edits BIGINT," +
								"minor_edits BIGINT," +
								"average_size BIGINT," +
								
								"distinct_pages_count BIGINT," +
								"distinct_editors_count INT," +
								"distinct_servers_count INT," +
								
								"smaller_origin BIGINT," +
								
								"PRIMARY KEY(id)" +
								");"
					);
            
            session.execute("CREATE TABLE IF NOT EXISTS results.servers_ranking" +
								"(id text," +
								"server_name TEXT," +
								"count INT," +
								"PRIMARY KEY((id), count, server_name)" +
								") WITH CLUSTERING ORDER BY (count DESC, server_name ASC);"
            		);
            
            session.execute("CREATE TABLE IF NOT EXISTS results." +
								"ranking" +
								"(id TEXT," +
								"name TEXT," +
								"count BIGINT," +
								
								"PRIMARY KEY((id), count, name)" +
								") WITH CLUSTERING ORDER BY (count DESC, name ASC);"
            		);
	}
	
	
	
	// Prepare the schema
	public void dropTables(Session session) {
	
		session.execute("DROP KEYSPACE IF EXISTS batch_views2");
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
					"Usage: java -cp <CLASSPATH> br.edu.ufcg.analytics.wikitrends.storage.serving2.CassandraServingLayer2Manager OPERATION <seed_address>");
			System.exit(1);
		}

		String operation = args[0];
		String seedNode = args[1];
		
		CassandraServingLayer2Manager manager = new CassandraServingLayer2Manager();
		
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
