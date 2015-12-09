package br.edu.ufcg.analytics.wikitrends.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.LocalDateTime;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import br.edu.ufcg.analytics.wikitrends.processing.batch1.TopEditorsBatch1;
import br.edu.ufcg.analytics.wikitrends.processing.batch1.TopIdiomsBatch1;
import br.edu.ufcg.analytics.wikitrends.processing.batch1.TopPagesBatch1;
import br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager;
import br.edu.ufcg.analytics.wikitrends.storage.serving1.CassandraServingLayer1Manager;

/**
 * @author Guilherme Gadelha
 * @author Ricardo Araújo Santos - ricoaraujosantos@gmail.com
 *
 */
public class SmallDataBatch1IT {

	private static final String TEST_CONFIGURATION_FILE = "src/test/resources/small_test_wikitrends.properties";
	private static final String INPUT_FILE = "src/test/resources/small_test_data.json";
	private static final String SEED_NODE = "localhost";

	private PropertiesConfiguration configuration;
	private Cluster cluster;
	private Session session;
	private CassandraMasterDatasetManager master_dataset_manager;
	private CassandraServingLayer1Manager serving_layer_manager;
	

	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		
		System.setProperty("spark.cassandra.connection.host", SEED_NODE);
		System.setProperty("spark.master", "local");
		System.setProperty("spark.app.name", "small-test");

		
		
		cluster = Cluster.builder().addContactPoints(SEED_NODE).build();
		session = cluster.newSession();
		
		
		configuration = new PropertiesConfiguration(TEST_CONFIGURATION_FILE);
		master_dataset_manager = new CassandraMasterDatasetManager();
		serving_layer_manager = new CassandraServingLayer1Manager();
		
		master_dataset_manager.dropTables(session);
		serving_layer_manager.dropTables(session);
		
		master_dataset_manager.createTables(session);
		serving_layer_manager.createTables(session);

		session.execute("USE batch_views");
		
		master_dataset_manager.populate(INPUT_FILE);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
//		master_dataset_manager.dropTables(session);
//		serving_layer_manager.dropTables(session);
		
		session.close();
		cluster.close();
	}

	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testProcessTopEditors() throws ConfigurationException {
		
		TopEditorsBatch1 job1 = new TopEditorsBatch1(configuration);
		job1.setCurrentTime(LocalDateTime.of(2015, 11, 9, 14, 00));//FIXME wrong date
		job1.process();
	
		try (Cluster cluster = Cluster.builder().addContactPoints(SEED_NODE).build();
				Session session = cluster.newSession();) {
			
			assertEquals(327, session.execute("SELECT count(1) FROM batch_views.top_editors").one().getLong("count"));
			assertEquals(510, session.execute("SELECT sum(count) as ranking_sum FROM batch_views.top_editors").one().getLong("ranking_sum"));

			long rankingMax = session.execute("SELECT max(count) as ranking_max FROM batch_views.top_editors").one().getLong("ranking_max");
			long rankingFirst = session.execute("SELECT count as ranking_max FROM batch_views.top_editors LIMIT 1").one().getLong("ranking_max");
			
			assertEquals(31, rankingMax);
			assertEquals(31, rankingFirst);
		}
	}
	
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testProcessTopPages() throws ConfigurationException {
		TopPagesBatch1 job2 = new TopPagesBatch1(configuration);
		job2.process();
		
		ResultSet resultSet = session.execute("SELECT count(1) FROM top_pages");
		assertEquals(490, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM top_pages WHERE count >= 3 ALLOW FILTERING");
		assertEquals(3, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM top_pages WHERE count = 2 ALLOW FILTERING");
		assertEquals(14, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT * FROM top_pages WHERE count = 3 AND name = 'Marie Antoinette' ALLOW FILTERING");
		List<Row> list = resultSet.all();
		assertTrue(list.size() == 1);
		assertTrue(list.get(0).getLong("count") == 3L);
		
		resultSet = session.execute("SELECT * FROM top_pages WHERE count = 3 AND name = 'Simone Zaza' ALLOW FILTERING");
		List<Row> list2 = resultSet.all();
		assertTrue(list2.size() == 1);
		assertTrue(list2.get(0).getString("name").equals("Simone Zaza"));
	}
	
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testProcessTopIdioms() throws ConfigurationException {
		TopIdiomsBatch1 job3 = new TopIdiomsBatch1(configuration);
		job3.process();
		
		ResultSet resultSet = session.execute("SELECT count(1) FROM top_idioms");
		assertEquals(45, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM top_idioms WHERE count >= 3 ALLOW FILTERING");
		assertEquals(19, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM top_idioms WHERE count = 2 ALLOW FILTERING");
		assertEquals(12, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT * FROM top_idioms WHERE count = 115 AND name = 'en.wikipedia.org' ALLOW FILTERING");
		List<Row> list = resultSet.all();
		assertTrue(list.size() == 1);
		assertTrue(list.get(0).getLong("count") == 115L);
		
		resultSet = session.execute("SELECT * FROM top_idioms WHERE count = 55 AND name = 'it.wikipedia.org' ALLOW FILTERING");
		List<Row> list2 = resultSet.all();
		assertTrue(list2.size() == 1);
		assertTrue(list2.get(0).getString("name").equals("it.wikipedia.org"));
	}
}
