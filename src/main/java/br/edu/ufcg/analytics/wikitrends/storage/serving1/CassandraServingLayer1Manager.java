package br.edu.ufcg.analytics.wikitrends.storage.serving1;

import java.io.Serializable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraServingLayer1Manager implements Serializable {

	/**
	 * SerialVersionUID for CassandraServingLayerManager
	 * 
	 *  @since November 26, 2015
	 */
	private static final long serialVersionUID = -1017103087942947022L;

	public void createTables(Session session) {
			session.execute("DROP KEYSPACE IF EXISTS batch_views");
		
			session.execute("CREATE KEYSPACE batch_views WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            
            session.execute("CREATE TABLE IF NOT EXISTS batch_views." +
								"top_editors" +
								
								"(name TEXT," +
								"count BIGINT," +
								
								"year INT," +
								"month INT," +
								"day INT," +
								"hour INT," +
								
								"PRIMARY KEY((year, month, day, hour), count, name)," +
								") WITH CLUSTERING ORDER BY (count DESC, name ASC);"
            		);
            
            session.execute("CREATE TABLE IF NOT EXISTS batch_views." +
								"top_idioms" +
								
								"(name TEXT," +
								"count BIGINT," +
								
								"year INT," +
								"month INT," +
								"day INT," +
								"hour INT," +
								
								"PRIMARY KEY((year, month, day, hour), count, name)," +
								") WITH CLUSTERING ORDER BY (count DESC, name ASC);"
            		);
           
            session.execute("CREATE TABLE IF NOT EXISTS batch_views." +
								"top_pages" +
								
								"(name TEXT," +
								"count BIGINT," +
								
								"year INT," +
								"month INT," +
								"day INT," +
								"hour INT," +

								"PRIMARY KEY((year, month, day, hour), count, name)," +
								") WITH CLUSTERING ORDER BY (count DESC, name ASC);"
            		);
            
            session.execute("CREATE TABLE IF NOT EXISTS batch_views." +
								"top_content_pages" +
								
								"(name TEXT," +
								"count BIGINT," +
								
								"year INT," +
								"month INT," +
								"day INT," +
								"hour INT," +
								
								"PRIMARY KEY((year, month, day, hour), count, name)," +
								") WITH CLUSTERING ORDER BY (count DESC, name ASC);"
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
            
            
            
            
            session.execute("CREATE TABLE IF NOT EXISTS batch_views.servers_ranking (" + 
            		"year INT," +
            		"month INT," +
            		"day INT," +
            		"hour INT," +
            		
            		"server_name TEXT," +
            		"number_of_changes INT," +
            		
            		"PRIMARY KEY((year, month, day, hour), number_of_changes, server_name)) " + 
            		"WITH CLUSTERING ORDER BY (number_of_changes DESC, server_name ASC);"
            		);
           
            session.execute("CREATE TABLE IF NOT EXISTS batch_views.status (" + 
            		"id TEXT," +
            		
            		"year INT," +
            		"month INT," +
            		"day INT," +
            		"hour INT," +
            		
            		"PRIMARY KEY((id), year, month, day, hour)) " + 
            		"WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC);"
            		);

	}



	/**
	 * Prepare the schema
	 * 
	 * @param session
	 */
	public void dropTables(Session session) {
	
		session.execute("DROP KEYSPACE IF EXISTS batch_views");
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
					"Usage: java -cp <CLASSPATH> br.edu.ufcg.analytics.wikitrends.storage.batch1.CassandraServingLayer1Manager OPERATION <seed_address>");
			System.exit(1);
		}

		String operation = args[0];
		String seedNode = args[1];

		CassandraServingLayer1Manager manager = new CassandraServingLayer1Manager();

		switch (operation) {
		case "CREATE":
			try (Cluster cluster = Cluster.builder().addContactPoints(seedNode).build();
					Session session = cluster.newSession();) {
				manager.createTables(session);
			}
			break;
		case "DROP":
			try (Cluster cluster = Cluster.builder().addContactPoints(seedNode).build();
					Session session = cluster.newSession();) {
				manager.dropTables(session);
			}
			break;
		default:
			System.err.println("Unsupported operation. Choose CREATE as operation.");
			break;
		}
	}
}