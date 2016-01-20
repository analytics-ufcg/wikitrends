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

	public void createAll(Session session) {
		
		createBatchViews1Keyspace(session);

		createTopEditorsTable(session);

        createTopIdiomsTable(session);
           
        createTopPagesTable(session);
            
        createTopContentPagesTable(session);
            
        createAbsValuesTable(session);

	}


	public void createBatchViews1Keyspace(Session session) {
		session.execute("CREATE KEYSPACE IF NOT EXISTS batch_views1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
	}



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



	public void createTopEditorsTable(Session session) {
		createTopTable(session, "top_editors");
	}



	public void createTopIdiomsTable(Session session) {
		session.execute("CREATE TABLE IF NOT EXISTS batch_views1." +
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
	}



	public void createTopPagesTable(Session session) {
		session.execute("CREATE TABLE IF NOT EXISTS batch_views1." +
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
	}



	public void createTopContentPagesTable(Session session) {
		session.execute("CREATE TABLE IF NOT EXISTS batch_views1." +
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
	}



	public void createAbsValuesTable(Session session) {
		session.execute("CREATE TABLE IF NOT EXISTS batch_views1." +
					            "absolute_values" +
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
			try (Cluster cluster = Cluster.builder().addContactPoints(seeds).build();
					Session session = cluster.newSession();) {
				switch(table) {
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
