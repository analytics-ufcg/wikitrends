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

	private static final String ABSOLUTE_VALUES_TABLE = "absolute_values";
	private static final String TOP_CONTENT_PAGES_TABLE = "top_content_pages";
	private static final String TOP_PAGES_TABLE = "top_pages";
	private static final String TOP_IDIOMS_TABLE = "top_idioms";
	private static final String TOP_EDITORS_TABLE = "top_editors";

	public void createAll(Session session) {
		createBatchViews1Keyspace(session);
		createTopTable(session, TOP_EDITORS_TABLE);
        createTopTable(session, TOP_IDIOMS_TABLE);
        createTopTable(session, TOP_PAGES_TABLE);
        createTopTable(session, TOP_CONTENT_PAGES_TABLE);
        createAbsValuesTable(session);
	}

	/**
	 * @param session
	 */
	public void createBatchViews1Keyspace(Session session) {
		session.execute("CREATE KEYSPACE IF NOT EXISTS batch_views1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
	}



	/**
	 * A "ranking" table is partitioned by date and ordered by count in decreasing order. 
	 * @param session Cassandra DB session
	 * @param name Table name
	 */
	public void createTopTable(Session session, String name) {
		session.execute("CREATE TABLE IF NOT EXISTS batch_views1." + 
								name + 
								"(name TEXT," +
								"count BIGINT," +
								
								"year INT," +
								"month INT," +
								"day INT," +
								"hour INT," +
								
								"PRIMARY KEY((year, month, day, hour), count, name)) " + 
								"WITH CLUSTERING ORDER BY (count DESC, name ASC);"
							);
	}



	public void createAbsValuesTable(Session session) {
		session.execute("CREATE TABLE IF NOT EXISTS batch_views1." +
					            ABSOLUTE_VALUES_TABLE +
								"(all_edits BIGINT," +
								"minor_edits BIGINT," +
								"average_size BIGINT," +
								
								"distinct_pages_set SET<TEXT>," +
								"distinct_editors_set SET<TEXT>," +
								"distinct_servers_set SET<TEXT>," +

								"smaller_origin BIGINT," +

								"year INT," +
								"month INT," +
								"day INT," +
								"hour INT," +

								"PRIMARY KEY((year, month, day, hour))"
								+ ")"
				);
	}



	/**
	 * Prepare the schema
	 * 
	 * @param session
	 */
	public void dropAll(Session session) {
	
		session.execute("DROP KEYSPACE IF EXISTS batch_views1");
	}
	
	public void dropTable(Session session, String table) {
		session.execute("DROP TABLE IF EXISTS batch_views1." + table);
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
					"Usage: java -cp <CLASSPATH> br.edu.ufcg.analytics.wikitrends.storage.serving1.CassandraServingLayer1Manager CREATE|DROP <ALL|TOP_EDITORS|TOP_IDIOMS|TOP_PAGES|TOP_CONTENT_PAGES|ABSOLUTE_VALUES>");
			System.exit(1);
		}

		String operation = args[0];
		String table = args[1];
		
		String[] seeds = System.getProperty("spark.cassandra.connection.host").split(",");


		CassandraServingLayer1Manager manager = new CassandraServingLayer1Manager();

		switch (operation) {
		case "CREATE":
			try (Cluster cluster = Cluster.builder().addContactPoints(seeds).build();
					Session session = cluster.newSession();) {
				manager.createBatchViews1Keyspace(session);
				switch(table) {
				case("TOP_EDITORS"):
					manager.createTopTable(session, TOP_EDITORS_TABLE);
					break;
				case("TOP_IDIOMS"):
					manager.createTopTable(session, TOP_IDIOMS_TABLE);
					break;
				case("TOP_PAGES"):
					manager.createTopTable(session, TOP_PAGES_TABLE);
					break;
				case("TOP_CONTENT_PAGES"):
					manager.createTopTable(session, TOP_CONTENT_PAGES_TABLE);
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
			try (Cluster cluster = Cluster.builder().addContactPoints(seeds).build();
					Session session = cluster.newSession();) {
				switch(table) {
				case("TOP_EDITORS"):
					manager.dropTable(session, TOP_EDITORS_TABLE);
					break;
				case("TOP_IDIOMS"):
					manager.dropTable(session, TOP_IDIOMS_TABLE);
					break;
				case("TOP_PAGES"):
					manager.dropTable(session, TOP_PAGES_TABLE);
					break;
				case("TOP_CONTENT_PAGES"):
					manager.dropTable(session, TOP_CONTENT_PAGES_TABLE);
					break;
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
