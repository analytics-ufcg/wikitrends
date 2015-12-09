package br.edu.ufcg.analytics.wikitrends.storage.raw;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

/**
 * Integration test for master dataset manager using cassandra database
 * 
 * @author Guilherme Gadelha
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 *
 */
public class CassandraMasterDatasetManagerIT {

	private static final String SEED_NODE = "localhost";
	private static final String INPUT_FILE = "src/test/resources/small_test_data.json";

	private Cluster cluster;
	private Session session;
	private CassandraMasterDatasetManager master_dataset_manager;


	@Before
	public void setUp() throws Exception {
		String[] testHosts = SEED_NODE.split(",");

		cluster = Cluster.builder().addContactPoints(testHosts).build();
		session = cluster.newSession();

		master_dataset_manager = new CassandraMasterDatasetManager();
	}

	@After
	public void stop() {
		session.close();
		cluster.close();
	}

	/**
	 * Test method for {@link br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager#createTables(Session)}.
	 */
	@Test
	public void testEmptyEditsTableCreation() {
		master_dataset_manager.dropTables(session);
		master_dataset_manager.createTables(session);
		session.execute("USE master_dataset;");
		ResultSet resultSet = session.execute("SELECT * FROM edits;");
		assertTrue(resultSet.all().isEmpty());
	}

	/**
	 * Test method for {@link br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager#createTables(Session)}.
	 */
	@Test
	public void testEmptyLogsTableCreation() {
		master_dataset_manager.dropTables(session);
		master_dataset_manager.createTables(session);
		session.execute("USE master_dataset;");
		ResultSet resultSet = session.execute("SELECT * FROM logs;");
		assertTrue(resultSet.all().isEmpty());
	}

	/**
	 * Test method for {@link br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager#populateFrom(String, String)}.
	 */
	@Test
	public void testPopulateEdits() {

		master_dataset_manager.createTables(session);
		
		System.setProperty("spark.cassandra.connection.host", SEED_NODE);
		System.setProperty("spark.master", "local");
		System.setProperty("spark.app.name", "migrate-test");

		master_dataset_manager.populate(INPUT_FILE);

		session.execute("USE master_dataset;");
		assertEquals(899, session.execute("SELECT count(1) FROM edits;").one().getLong("count"));
	}

	/**
	 * Test method for {@link br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager#populateFrom(String, String)}.
	 */
	@Test
	public void testPopulateLogs() {

		master_dataset_manager.createTables(session);

		System.setProperty("spark.cassandra.connection.host", SEED_NODE);
		System.setProperty("spark.master", "local");
		System.setProperty("spark.app.name", "migrate-test");

		master_dataset_manager.populate(INPUT_FILE);

		session.execute("USE master_dataset;");
		assertEquals(101, session.execute("SELECT count(1) FROM logs;").one().getLong("count"));
	}
}
