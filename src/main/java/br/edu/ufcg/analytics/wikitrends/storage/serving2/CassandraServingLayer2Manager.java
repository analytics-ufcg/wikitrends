package br.edu.ufcg.analytics.wikitrends.storage.serving2;

import java.io.Serializable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraServingLayer2Manager implements Serializable {
	

	private static final String ABSOLUTE_VALUES_TABLE = "absolute_values";
	private static final String RANKING_TABLE = "rankings";
	/**
	 * SerialVersionUID for CassandraResultsLayerManager
	 * 
	 * @since November 25, 2015
	 */
	private static final long serialVersionUID = -1465057423342253096L;

	// Prepare the schema
	public void createAll(Session session) {
		createBatchViews2Keyspace(session);
		createTopTable(session, RANKING_TABLE);
		createAbsValuesTable(session);
	}

	public void createAbsValuesTable(Session session) {
		session.execute("CREATE TABLE IF NOT EXISTS batch_views2." +
				            ABSOLUTE_VALUES_TABLE +
							"(" + 
							"id TEXT," +
							"name TEXT," +
							"value BIGINT," +
							"PRIMARY KEY((id), name)" +
							") WITH CLUSTERING ORDER BY (name ASC);"
				);
	}

	public void createTopTable(Session session, String table) {
		session.execute("CREATE TABLE IF NOT EXISTS batch_views2."
				+ table +
				"(id TEXT," +
				"position INT," +
				"name TEXT," +
				"count BIGINT," +
				"PRIMARY KEY((id), position)" +
				") WITH CLUSTERING ORDER BY (position ASC);"
				);
	}

	public void createBatchViews2Keyspace(Session session) {
		session.execute("CREATE KEYSPACE IF NOT EXISTS batch_views2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}");
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
					"Usage: java -cp <CLASSPATH> br.edu.ufcg.analytics.wikitrends.storage.serving2.CassandraServingLayer2Manager CREATE|DROP <ALL|TOP_EDITORS|TOP_IDIOMS|TOP_PAGES|TOP_CONTENT_PAGES|ABSOLUTE_VALUES>");
			System.exit(1);
		}

		String operation = args[0];
		String table = args[1];
		
		String[] seeds = System.getProperty("spark.cassandra.connection.host").split(",");

		CassandraServingLayer2Manager manager = new CassandraServingLayer2Manager();
		
		switch (operation) {
		case "CREATE":
			try (Cluster cluster = Cluster.builder().addContactPoints(seeds).build();
					Session session = cluster.newSession();) {
				manager.createBatchViews2Keyspace(session);
				switch(table) {
//				case("TOP_EDITORS"):
//					manager.createTopTable(session, TOP_EDITORS_TABLE);
//					break;
//				case("TOP_IDIOMS"):
//					manager.createTopTable(session, TOP_IDIOMS_TABLE);
//					break;
//				case("TOP_PAGES"):
//					manager.createTopTable(session, TOP_PAGES_TABLE);
//					break;
//				case("TOP_CONTENT_PAGES"):
//					manager.createTopTable(session, TOP_CONTENT_PAGES_TABLE);
//					break;
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
			try (Cluster cluster = Cluster.builder().addContactPoints(seeds).build();
					Session session = cluster.newSession();) {
				switch(table) {
//				case("TOP_EDITORS"):
//					manager.dropTable(session, TOP_EDITORS_TABLE);
//					break;
//				case("TOP_IDIOMS"):
//					manager.dropTable(session, TOP_IDIOMS_TABLE);
//					break;
//				case("TOP_PAGES"):
//					manager.dropTable(session, TOP_PAGES_TABLE);
//					break;
//				case("TOP_CONTENT_PAGES"):
//					manager.dropTable(session, TOP_CONTENT_PAGES_TABLE);
//					break;
				case("ABSOLUTE_VALUES"):
					manager.dropTable(session, ABSOLUTE_VALUES_TABLE);
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
