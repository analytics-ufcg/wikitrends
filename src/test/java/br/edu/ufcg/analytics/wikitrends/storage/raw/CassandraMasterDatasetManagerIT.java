package br.edu.ufcg.analytics.wikitrends.storage.raw;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

/**
 * Integration test for master dataset manager using cassandra database
 * 
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 *
 */
public class CassandraMasterDatasetManagerIT {
	
	private static String seedNode = "localhost";
	private static String inputFile = "src/test/resources/small_test_data.json";

	/**
	 * Test method for {@link br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager#createTables(Session)}.
	 */
	@Test
	public void testEmptyEditsTableCreation() {
		String[] testHosts = seedNode.split(",");
		try(Cluster cluster = Cluster.builder().addContactPoints(testHosts).build();){
			
			try(Session session = cluster.newSession();){
				new CassandraMasterDatasetManager().dropTables(session);
				new CassandraMasterDatasetManager().createTables(session);
				session.execute("USE master_dataset;");
				ResultSet resultSet = session.execute("SELECT * FROM edits;");
				assertTrue(resultSet.all().isEmpty());
			}
			
		}
	}

	/**
	 * Test method for {@link br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager#createTables(Session)}.
	 */
	@Test
	public void testEmptyLogsTableCreation() {
		String[] testHosts = seedNode.split(",");
		try(Cluster cluster = Cluster.builder().addContactPoints(testHosts).build();){
			
			try(Session session = cluster.newSession();){
				new CassandraMasterDatasetManager().dropTables(session);
				new CassandraMasterDatasetManager().createTables(session);
				session.execute("USE master_dataset;");
				ResultSet resultSet = session.execute("SELECT * FROM logs;");
				assertTrue(resultSet.all().isEmpty());
			}
			
		}
	}
	
	/**
	 * Test method for {@link br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager#populateFrom(String, String)}.
	 */
	@Test
	public void testPopulateEdits() {
		String[] testHosts = seedNode.split(",");
		try(Cluster cluster = Cluster.builder().addContactPoints(testHosts).build();){
			
			CassandraMasterDatasetManager manager = new CassandraMasterDatasetManager();
			
			try(Session session = cluster.newSession();){
				manager.createTables(session);
			}
			
			
			SparkConf conf = new SparkConf();
			conf.set("spark.cassandra.connection.host", seedNode);

			try (JavaSparkContext sc = new JavaSparkContext("local", "test", conf);) {
				manager.populateFrom(seedNode, inputFile, sc);
			}
			
			try(Session session = cluster.newSession();){
				session.execute("USE master_dataset;");
				assertEquals(899, session.execute("SELECT count(1) FROM edits;").one().getLong("count"));
			}
			
		}
	}

	/**
	 * Test method for {@link br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager#populateFrom(String, String)}.
	 */
	@Test
	public void testPopulateLogs() {
		String[] testHosts = seedNode.split(",");
		try(Cluster cluster = Cluster.builder().addContactPoints(testHosts).build();){
			
			CassandraMasterDatasetManager manager = new CassandraMasterDatasetManager();
			
			try(Session session = cluster.newSession();){
				manager.createTables(session);
			}
			
			
			SparkConf conf = new SparkConf();
			conf.set("spark.cassandra.connection.host", seedNode);

			try (JavaSparkContext sc = new JavaSparkContext("local", "test", conf);) {
				manager.populateFrom(seedNode, inputFile, sc);
			}
			
			try(Session session = cluster.newSession();){
				session.execute("USE master_dataset;");
				assertEquals(101, session.execute("SELECT count(1) FROM logs;").one().getLong("count"));
			}
			
		}
	}

}
