/**
 * 
 */
package br.edu.ufcg.analytics.wikitrends.integration;

import static org.junit.Assert.assertEquals;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import br.edu.ufcg.analytics.wikitrends.processing.batch1.CassandraIncrementalBatchLayer1Job;
import br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager;
import br.edu.ufcg.analytics.wikitrends.storage.serving1.CassandraServingLayer1Manager;

/**
 * @author Ricardo Araújo Santos - ricoaraujosantos@gmail.com
 *
 */
public class SmallDataBatch1IT {

	private static final String SEED_NODE = "localhost";
	private static final String INPUT_FILE = "src/test/resources/small_test_data.json";
	private static final String TEST_CONFIGURATION_FILE = "src/test/resources/wikitrends.properties";


	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		String[] testHosts = SEED_NODE.split(",");
		try(Cluster cluster = Cluster.builder().addContactPoints(testHosts).build();){

			CassandraMasterDatasetManager manager = new CassandraMasterDatasetManager();
			CassandraServingLayer1Manager serving = new CassandraServingLayer1Manager();

			try(Session session = cluster.newSession();){
				manager.dropTables(session);
				manager.createTables(session);
				
				serving.dropTables(session);
				serving.createTables(session);
			}


			SparkConf conf = new SparkConf();
			conf.set("spark.cassandra.connection.host", SEED_NODE);

			try (JavaSparkContext sc = new JavaSparkContext("local", "test", conf);) {
				manager.populateFrom(SEED_NODE, INPUT_FILE, sc);
			}
		}
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
//		String[] testHosts = SEED_NODE.split(",");
//		try(Cluster cluster = Cluster.builder().addContactPoints(testHosts).build();
//			Session session = cluster.newSession();){
//
//			new CassandraMasterDatasetManager().dropTables(session);
//			new CassandraServingLayer1Manager().dropTables(session);
//		}
	}

	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testProcessEditorsRanking() throws ConfigurationException {
		Configuration configuration = new PropertiesConfiguration(TEST_CONFIGURATION_FILE);
		CassandraIncrementalBatchLayer1Job job = new CassandraIncrementalBatchLayer1Job(configuration);
		
		
		SparkConf conf = new SparkConf();
		conf.set("spark.cassandra.connection.host", "localhost");
		try(JavaSparkContext sc = new JavaSparkContext("local", "small-data-batch1-test", conf);){
			job.processEditorsRanking(sc);
		}	
		
		try (Cluster cluster = Cluster.builder().addContactPoints(SEED_NODE).build();
				Session session = cluster.newSession();) {
			
			ResultSet resultSet = session.execute("SELECT count(1) FROM master_dataset.edits WHERE year = ? AND month = ? AND day = ? AND hour = ? ALLOW FILTERING", 2015, 11, 9, 11);
			assertEquals(899, resultSet.one().getLong("count"));
			
			resultSet = session.execute("SELECT count(1) FROM master_dataset.logs");
			assertEquals(101, resultSet.one().getLong("count"));

			resultSet = session.execute("SELECT count(1) FROM batch_views.top_editors");
			assertEquals(11, resultSet.one().getLong("count"));
		}
	}
	
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testProcessTopPages() throws ConfigurationException {
		Configuration configuration = new PropertiesConfiguration(TEST_CONFIGURATION_FILE);
		CassandraIncrementalBatchLayer1Job job = new CassandraIncrementalBatchLayer1Job(configuration);
		
		SparkConf conf = new SparkConf();
		conf.set("spark.cassandra.connection.host", "localhost");
		try(JavaSparkContext sc = new JavaSparkContext("local", "small-data-batch1-test", conf);){
			job.processTopPages(sc);
		}	
		
		try (Cluster cluster = Cluster.builder().addContactPoints(SEED_NODE).build();
				Session session = cluster.newSession();) {
			
			ResultSet resultSet = session.execute("SELECT count(1) FROM master_dataset.edits WHERE year = ? and month = ? and day = ? and hour = ? ALLOW FILTERING", 2015, 11, 9, 11);
			assertEquals(899, resultSet.one().getLong("count"));
			
			resultSet = session.execute("SELECT count(1) FROM master_dataset.logs");
			assertEquals(101, resultSet.one().getLong("count"));

			resultSet = session.execute("SELECT count(1) FROM batch_views.top_pages");
			assertEquals(11, resultSet.one().getLong("count"));
		}
	}

}
