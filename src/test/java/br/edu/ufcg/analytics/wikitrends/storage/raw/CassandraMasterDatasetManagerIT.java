package br.edu.ufcg.analytics.wikitrends.storage.raw;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
		master_dataset_manager.dropTables(session);
		master_dataset_manager.createTables(session);
		session.execute("USE master_dataset;");

	}

	@After
	public void stop() {
		
		master_dataset_manager.dropTables(session);
		
		session.close();
		cluster.close();
	}

	/**
	 * Test method for {@link br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager#createTables(Session)}.
	 */
	@Test
	public void testEmptyEditsTableCreation() {
		ResultSet resultSet = session.execute("SELECT * FROM edits;");
		assertTrue(resultSet.all().isEmpty());
	}

	/**
	 * Test method for {@link br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager#createTables(Session)}.
	 */
	@Test
	public void testEmptyLogsTableCreation() {
		ResultSet resultSet = session.execute("SELECT * FROM logs;");
		assertTrue(resultSet.all().isEmpty());
	}

	/**
	 * Test method for {@link br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager#populateFrom(String, String)}.
	 */
	@Test
	public void testPopulateEdits() {

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

		System.setProperty("spark.cassandra.connection.host", SEED_NODE);
		System.setProperty("spark.master", "local");
		System.setProperty("spark.app.name", "migrate-test");

		master_dataset_manager.populate(INPUT_FILE);

		session.execute("USE master_dataset;");
		assertEquals(101, session.execute("SELECT count(1) FROM logs;").one().getLong("count"));
	}
}
