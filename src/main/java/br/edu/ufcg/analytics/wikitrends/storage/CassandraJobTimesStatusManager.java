package br.edu.ufcg.analytics.wikitrends.storage;

import java.io.Serializable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraJobTimesStatusManager implements Serializable {

	/**
	 * SerialVersionUID for CassandraServingLayerManager
	 * 
	 *  @since November 26, 2015
	 */
	private static final long serialVersionUID = -1017103087942947022L;

	public void createTables(Session session) {
		
		session.execute("CREATE KEYSPACE job_times WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

        session.execute("CREATE TABLE IF NOT EXISTS job_times.status (" + 
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
	
		session.execute("DROP KEYSPACE IF EXISTS job_times");
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
					"Usage: java -cp <CLASSPATH> br.edu.ufcg.analytics.wikitrends.storage.CassandraJobTimesStatusManager OPERATION <seed_address>");
			System.exit(1);
		}

		String operation = args[0];
		String seedNode = args[1];

		CassandraJobTimesStatusManager manager = new CassandraJobTimesStatusManager();

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
