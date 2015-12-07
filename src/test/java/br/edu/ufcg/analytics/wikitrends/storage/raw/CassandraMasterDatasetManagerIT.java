package br.edu.ufcg.analytics.wikitrends.storage.raw;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.configuration.PropertiesConfiguration;
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
	
	private static final String TEST_CONFIGURATION_FILE = "src/test/resources/small_test_wikitrends.properties";
	private PropertiesConfiguration configuration;
	private Cluster cluster;
	private Session session;
	private CassandraMasterDatasetManager master_dataset_manager;

	@Before
	public void setUp() throws Exception {
		configuration = new PropertiesConfiguration(TEST_CONFIGURATION_FILE);
		
		String[] testHosts = configuration.getString("spark.cassandra.connection.host").split(",");
		cluster = Cluster.builder().addContactPoints(testHosts).build();
		
		master_dataset_manager = new CassandraMasterDatasetManager(new PropertiesConfiguration("src/test/resources/small_test_wikitrends.properties"));
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
		try(Session session = cluster.newSession();){
			master_dataset_manager.dropTables(session);
			master_dataset_manager.createTables(session);
			session.execute("USE master_dataset;");
			ResultSet resultSet = session.execute("SELECT * FROM edits;");
			assertTrue(resultSet.all().isEmpty());
		}
	}

	/**
	 * Test method for {@link br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager#createTables(Session)}.
	 */
	@Test
	public void testEmptyLogsTableCreation() {
		try(Session session = cluster.newSession();){
			master_dataset_manager.dropTables(session);
			master_dataset_manager.createTables(session);
			session.execute("USE master_dataset;");
			ResultSet resultSet = session.execute("SELECT * FROM logs;");
			assertTrue(resultSet.all().isEmpty());
		}
	}
	
	/**
	 * Test method for {@link br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager#populateFrom(String, String)}.
	 */
	@Test
	public void testPopulateEdits() {
			
		try(Session session = cluster.newSession();){
			master_dataset_manager.createTables(session);
		}
		
		master_dataset_manager.populate();
		
		try(Session session = cluster.newSession();){
			session.execute("USE master_dataset;");
			assertEquals(899, session.execute("SELECT count(1) FROM edits;").one().getLong("count"));
		}
	}

	/**
	 * Test method for {@link br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager#populateFrom(String, String)}.
	 */
	@Test
	public void testPopulateLogs() {
			
		try(Session session = cluster.newSession();){
			master_dataset_manager.createTables(session);
		}
		
		master_dataset_manager.populate();
		
		try(Session session = cluster.newSession();){
			session.execute("USE master_dataset;");
			assertEquals(101, session.execute("SELECT count(1) FROM logs;").one().getLong("count"));
		}
	}
}
