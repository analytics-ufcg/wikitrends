package br.edu.ufcg.analytics.wikitrends.integration;

import static org.junit.Assert.assertEquals;

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

import br.edu.ufcg.analytics.wikitrends.processing.batch1.TopEditorsBatch1;
import br.edu.ufcg.analytics.wikitrends.processing.batch1.TopPagesBatch1;
import br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager;
import br.edu.ufcg.analytics.wikitrends.storage.serving1.CassandraServingLayer1Manager;

/**
 * @author Guilherme Gadelha
 * @author Ricardo Araújo Santos - ricoaraujosantos@gmail.com
 *
 */
public class SmallDataBatch1IT {

	private static final String SEED_NODE = "localhost";
	private static final String INPUT_FILE = "src/test/resources/small_test_data.json";
	private static final String TEST_CONFIGURATION_FILE = "src/test/resources/wikitrends.properties";
	private JavaSparkContext sc;
	private Cluster cluster;
	private Session session;
	private CassandraMasterDatasetManager master_dataset_manager;
	private CassandraServingLayer1Manager serving_layer_manager;
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		String[] testHosts = SEED_NODE.split(",");

		SparkConf conf = new SparkConf();
		conf.set("spark.cassandra.connection.host", SEED_NODE);
		sc = new JavaSparkContext("local", "small-data-batch1-test", conf);

		cluster = Cluster.builder().addContactPoints(testHosts).build();
		
		master_dataset_manager = new CassandraMasterDatasetManager();
		serving_layer_manager = new CassandraServingLayer1Manager();
		
		session = cluster.newSession();
		
		master_dataset_manager.dropTables(session);
		master_dataset_manager.createTables(session);
		
		serving_layer_manager.dropTables(session);
		serving_layer_manager.createTables(session);

		master_dataset_manager.populateFrom(SEED_NODE, INPUT_FILE, sc);
		
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
//		master_dataset_manager.dropTables(session);
//		serving_layer_manager.dropTables(session);
		
		sc.close();
		session.close();
		cluster.close();
	}

	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testProcessEditorsRanking() throws ConfigurationException {
		TopEditorsBatch1 job1 = new TopEditorsBatch1(new PropertiesConfiguration(TEST_CONFIGURATION_FILE));
		job1.process(sc);
		
		assertEquals(11, session.execute("SELECT count(1) FROM batch_views.top_editors").one().getLong("count"));
		assertEquals(126, session.execute("SELECT sum(count) as ranking_sum FROM batch_views.top_editors").one().getLong("ranking_sum"));
	}
	
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testProcessTopPages() throws ConfigurationException {
		TopPagesBatch1 job2 = new TopPagesBatch1(new PropertiesConfiguration(TEST_CONFIGURATION_FILE));
		job2.process(sc);
		
		ResultSet resultSet = session.execute("SELECT count(1) FROM master_dataset.edits WHERE year = ? and month = ? and day = ? and hour = ? ALLOW FILTERING", 2015, 11, 9, 11);
		assertEquals(899, resultSet.one().getLong("count"));
			
		resultSet = session.execute("SELECT count(1) FROM master_dataset.logs");
		assertEquals(101, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(1) FROM batch_views.top_pages");
		assertEquals(3, resultSet.one().getLong("count"));
	
	}
}
