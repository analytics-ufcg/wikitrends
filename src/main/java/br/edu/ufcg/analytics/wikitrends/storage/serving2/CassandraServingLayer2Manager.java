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
	public void createAll(Session session) {
                    
            createBatchViews2Keyspace(session);
            
            createTopEditorsTable(session);
            
            createTopIdiomsTable(session);
           
            createTopPagesTable(session);
            
            createTopContentPagesTable(session);
            
            createAbsValuesTable(session);

	}



	public void createAbsValuesTable(Session session) {
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
	}



	public void createTopContentPagesTable(Session session) {
		session.execute("CREATE TABLE IF NOT EXISTS batch_views2.top_content_pages" +
							"(id TEXT," +
							"name TEXT," +
							"count BIGINT," +
							"PRIMARY KEY((id), count, name)" +
							") WITH CLUSTERING ORDER BY (count DESC, name ASC);"
				);
	}



	public void createTopPagesTable(Session session) {
		session.execute("CREATE TABLE IF NOT EXISTS batch_views2.top_pages" +
							"(id TEXT," +
							"name TEXT," +
							"count BIGINT," +
							"PRIMARY KEY((id), count, name)" +
							") WITH CLUSTERING ORDER BY (count DESC, name ASC);"
				);
	}



	public void createTopIdiomsTable(Session session) {
		session.execute("CREATE TABLE IF NOT EXISTS batch_views2.top_idioms" +
							"(id TEXT," +
							"name TEXT," +
							"count BIGINT," +
							"PRIMARY KEY((id), count, name)" +
							") WITH CLUSTERING ORDER BY (count DESC, name ASC);"
				);
	}



	public void createTopEditorsTable(Session session) {
		session.execute("CREATE TABLE IF NOT EXISTS batch_views2.top_editors" +
							"(id TEXT," +
							"name TEXT," +
							"count BIGINT," +
							"PRIMARY KEY((id), count, name)" +
							") WITH CLUSTERING ORDER BY (count DESC, name ASC);"
				);
	}



	public void createBatchViews2Keyspace(Session session) {
		session.execute("CREATE KEYSPACE IF NOT EXISTS batch_views2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
	}
	
	
	
	// Prepare the schema
	public void dropAll(Session session) {
	
		session.execute("DROP KEYSPACE IF EXISTS batch_views2");
	}
	
	public void dropTable(Session session, String table) {
		session.execute("DROP TABLE IF EXISTS batch_views2." +  table);
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
					"Usage: java -cp <CLASSPATH> br.edu.ufcg.analytics.wikitrends.storage.serving2.CassandraServingLayer2Manager OPERATION [<table>] <seed_address>");
			System.exit(1);
		}

		String operation = args[0];
		String operation2 = args[1];
		String seedNode = args[2];
		
		CassandraServingLayer2Manager manager = new CassandraServingLayer2Manager();
		
		switch (operation) {
		case "CREATE":
			try (Cluster cluster = Cluster.builder().addContactPoints(seedNode).build();
					Session session = cluster.newSession();) {
				manager.createBatchViews2Keyspace(session);
				switch(operation2) {
				case("TOP_EDITORS"):
					manager.createTopEditorsTable(session);
					break;
				case("TOP_IDIOMS"):
					manager.createTopIdiomsTable(session);
					break;
				case("TOP_PAGES"):
					manager.createTopPagesTable(session);
					break;
				case("TOP_CONTENT_PAGES"):
					manager.createTopContentPagesTable(session);
					break;
				case("ABSOLUTE_VALUES"):
					manager.createAbsValuesTable(session);
					break;
				default:
					manager.createAll(session);
					break;
				}
			}
			break;
		case "DROP":
			try (Cluster cluster = Cluster.builder().addContactPoints(seedNode).build();
					Session session = cluster.newSession();) {
				switch(operation2) {
				case("TOP_EDITORS"):
					manager.dropTable(session, "top_editors");
					break;
				case("TOP_IDIOMS"):
					manager.dropTable(session, "top_idioms");
					break;
				case("TOP_PAGES"):
					manager.dropTable(session, "top_pages");
					break;
				case("TOP_CONTENT_PAGES"):
					manager.dropTable(session, "top_content_pages");
					break;
				case("ABSOLUTE_VALUES"):
					manager.dropTable(session, "absolute_values");
					break;
				default:
					manager.dropAll(session);
					break;
				}
			}
			break;
		default:
			System.err.println("Unsupported operation. Choose CREATE or DROP as operation.");
			break;
		}

	}
}
