package br.edu.ufcg.analytics.wikitrends.storage.batchview;

import java.io.Serializable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraBatchViewsManager implements Serializable {


	private enum Operation{
		CREATE, DROP
	}

	private enum Table{
		ALL,
		CONTENT,
		EDITORS, 
		IDIOMS, 
		METRICS, 
		PAGES,
		STATUS
	}

	/**
	 * SerialVersionUID for CassandraServingLayerManager
	 * 
	 *  @since November 26, 2015
	 */
	private static final long serialVersionUID = -1017103087942947022L;

	private static final String KEYSPACE = "batch_views";

	private static final String STATUS_TABLE = "status";

	private static final String PARTIAL_METRICS_TABLE = "partial_metrics";
	private static final String CONTENT_PAGES_PARTIAL_RANKINGS_TABLE = "content_pages_partial_rankings";
	private static final String PAGES_PARTIAL_RANKINGS_TABLE = "pages_partial_rankings";
	private static final String IDIOMS_PARTIAL_RANKINGS_TABLE = "idioms_partial_rankings";
	private static final String EDITORS_PARTIAL_RANKINGS_TABLE = "editors_partial_rankings";

	private static final String FINAL_METRICS_TABLE = "final_metrics";
	private static final String FINAL_RANKINGS_TABLE = "final_rankings";


	public void createAll(Session session) {
		createKeyspace(session);
		createRankingTables(session, EDITORS_PARTIAL_RANKINGS_TABLE);
		createRankingTables(session, IDIOMS_PARTIAL_RANKINGS_TABLE);
		createRankingTables(session, PAGES_PARTIAL_RANKINGS_TABLE);
		createRankingTables(session, CONTENT_PAGES_PARTIAL_RANKINGS_TABLE);
		createStatusTable(session);
		createMetricsTable(session);
	}

	/**
	 * @param session
	 */
	public void createKeyspace(Session session) {
		session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE
				+ " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}");
	}



	/**
	 * A "ranking" table is partitioned by date and ordered by count in decreasing order. 
	 * @param session Cassandra DB session
	 * @param partialRankingName Table name
	 */
	public void createRankingTables(Session session, String partialRankingName) {
		session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + 
				partialRankingName + 
				"(name TEXT," +
				"count BIGINT," +
				"year INT," +
				"month INT," +
				"day INT," +
				"hour INT," +
				"PRIMARY KEY((year, month, day, hour), count, name)) " + 
				"WITH CLUSTERING ORDER BY (count DESC, name ASC);"
				);
		session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + "."
				+ FINAL_RANKINGS_TABLE +
				"(id TEXT," +
				"position INT," +
				"name TEXT," +
				"count BIGINT," +
				"PRIMARY KEY((id), position)" +
				") WITH CLUSTERING ORDER BY (position ASC);"
				);

	}



	public void createMetricsTable(Session session) {
		session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." +
				PARTIAL_METRICS_TABLE +
				"("+ 
				"name TEXT," +
				"value BIGINT," +
				"year INT," +
				"month INT," +
				"day INT," +
				"hour INT," +
				"PRIMARY KEY((year, month, day, hour), name)"
				+ ")"
				);
		session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." +
				FINAL_METRICS_TABLE +
				"(" + 
				"id TEXT," +
				"name TEXT," +
				"value BIGINT," +
				"PRIMARY KEY((id), name)" +
				") WITH CLUSTERING ORDER BY (name ASC);"
				);
	}

	public void createStatusTable(Session session) {

		session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + STATUS_TABLE + " (" + 
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
	public void dropAll(Session session) {
		session.execute("DROP KEYSPACE IF EXISTS " + KEYSPACE);
	}

	public void dropTable(Session session, String table) {
		session.execute("DROP TABLE IF EXISTS " + KEYSPACE + "." + table);
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

		Operation operation = Operation.valueOf(args[0].toUpperCase());
		Table table = Table.valueOf(args[1].toUpperCase());

		String[] seeds = System.getProperty("spark.cassandra.connection.host").split(",");


		CassandraBatchViewsManager manager = new CassandraBatchViewsManager();

		switch (operation) {
		case CREATE:
			try (Cluster cluster = Cluster.builder().addContactPoints(seeds).build();
					Session session = cluster.newSession();) {
				manager.createKeyspace(session);
				switch(table) {
				case ALL:
					manager.createAll(session);
					break;
				case EDITORS:
					manager.createRankingTables(session, EDITORS_PARTIAL_RANKINGS_TABLE);
					break;
				case IDIOMS:
					manager.createRankingTables(session, IDIOMS_PARTIAL_RANKINGS_TABLE);
					break;
				case PAGES:
					manager.createRankingTables(session, PAGES_PARTIAL_RANKINGS_TABLE);
					break;
				case CONTENT:
					manager.createRankingTables(session, CONTENT_PAGES_PARTIAL_RANKINGS_TABLE);
					break;
				case METRICS:
					manager.createMetricsTable(session);
					break;
				case STATUS:
					manager.createStatusTable(session);
					break;
				}
			}
			break;
		case DROP:
			try (Cluster cluster = Cluster.builder().addContactPoints(seeds).build();
					Session session = cluster.newSession();) {
				switch(table) {
				case ALL:
					manager.dropAll(session);
					break;
				case EDITORS:
					manager.dropTable(session, EDITORS_PARTIAL_RANKINGS_TABLE);
					break;
				case IDIOMS:
					manager.dropTable(session, IDIOMS_PARTIAL_RANKINGS_TABLE);
					break;
				case PAGES:
					manager.dropTable(session, PAGES_PARTIAL_RANKINGS_TABLE);
					break;
				case CONTENT:
					manager.dropTable(session, CONTENT_PAGES_PARTIAL_RANKINGS_TABLE);
					break;
				case METRICS:
					manager.dropTable(session, FINAL_METRICS_TABLE);
					break;
				case STATUS:
					manager.dropTable(session, STATUS_TABLE);
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
