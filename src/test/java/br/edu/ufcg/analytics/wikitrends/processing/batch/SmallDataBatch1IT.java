package br.edu.ufcg.analytics.wikitrends.processing.batch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.LocalDateTime;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import br.edu.ufcg.analytics.wikitrends.storage.batchview.CassandraBatchViewsManager;
import br.edu.ufcg.analytics.wikitrends.storage.master.CassandraMasterDatasetManager;

/**
 * @author Guilherme Gadelha
 * @author Ricardo Araújo Santos - ricoaraujosantos@gmail.com
 * 
 * Set of tests the runs the process of Phase 1 from the Workflow : Batch1.
 * It basically calls the method process() from a job built with
 * a starttime/currenttime set on the test.
 *
 */
public class SmallDataBatch1IT {

	private static final String TEST_CONFIGURATION_FILE = "src/test/resources/small_test_wikitrends.properties"; //FIXME update me!!!!
	private static final String INPUT_FILE = "src/test/resources/small_test_data.json";
	private static final String SEED_NODE = "localhost";

	private PropertiesConfiguration configuration;
	private Cluster cluster;
	private Session session;

	private static LocalDateTime currentTime;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void prepareMasterDataset() throws Exception {
		System.setProperty("spark.cassandra.connection.host", SEED_NODE);
		System.setProperty("spark.master", "local");
		System.setProperty("spark.app.name", "small-test");
		System.setProperty("spark.driver.allowMultipleContexts", "true");
		System.setProperty("spark.cassandra.output.consistency.level", "LOCAL_ONE");
		  
		cleanMasterDataset();

		try(
				Cluster cluster = Cluster.builder().addContactPoints(SEED_NODE).build();
				Session session = cluster.newSession();
				){
			
			new CassandraMasterDatasetManager().dropAll(session);
			new CassandraBatchViewsManager().dropAll(session);
			
			new CassandraMasterDatasetManager().createAll(session);
			new CassandraBatchViewsManager().createAll(session);

		}

		new CassandraMasterDatasetManager().populate(INPUT_FILE);
		currentTime = LocalDateTime.of(2015, 11, 9, 14, 00);
	}
	
	/**
	 * Clean master dataset
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void cleanMasterDataset() throws Exception {

		try (Cluster cluster = Cluster.builder().addContactPoints(SEED_NODE).build();
				Session session = cluster.newSession();) {
			new CassandraMasterDatasetManager().dropAll(session);
			new CassandraBatchViewsManager().dropAll(session);
		}

	}


	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void openCassandraSession() throws Exception {

		configuration = new PropertiesConfiguration(TEST_CONFIGURATION_FILE);

		cluster = Cluster.builder().addContactPoints(SEED_NODE).build();
		session = cluster.newSession();
		session.execute("USE batch_views");

	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void closeCassandraSession() throws Exception {
		session.close();
		cluster.close();
	}

	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testProcessTopEditors() throws ConfigurationException {
		
		EditorsPartialRankingBatchJob job = new EditorsPartialRankingBatchJob(configuration);
		job.process(currentTime);

		assertEquals(327, session.execute("SELECT count(1) FROM batch_views.editors_partial_rankings").one().getLong("count"));
		assertEquals(510, session.execute("SELECT sum(count) as ranking_sum FROM batch_views.editors_partial_rankings").one().getLong("ranking_sum"));

		long rankingMax = session.execute("SELECT max(count) as ranking_max FROM batch_views.editors_partial_rankings").one().getLong("ranking_max");
		long rankingFirst = session.execute("SELECT count as ranking_max FROM batch_views.editors_partial_rankings LIMIT 1").one().getLong("ranking_max");

		assertEquals(31, rankingMax);
		assertEquals(31, rankingFirst);
	}
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testProcessTopIdioms() throws ConfigurationException {
		IdiomsPartialRankingBatchJob job = new IdiomsPartialRankingBatchJob(configuration);
		job.process(currentTime);
		
		ResultSet resultSet = session.execute("SELECT count(1) FROM idioms_partial_rankings");
		assertEquals(45, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM idioms_partial_rankings WHERE count >= 3 ALLOW FILTERING");
		assertEquals(19, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM idioms_partial_rankings WHERE count = 2 ALLOW FILTERING");
		assertEquals(12, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT * FROM idioms_partial_rankings WHERE count = 115 AND name = 'en.wikipedia.org' ALLOW FILTERING");
		List<Row> list = resultSet.all();
		assertTrue(list.size() == 1);
		assertTrue(list.get(0).getLong("count") == 115L);
		
		resultSet = session.execute("SELECT * FROM idioms_partial_rankings WHERE count = 55 AND name = 'it.wikipedia.org' ALLOW FILTERING");
		List<Row> list2 = resultSet.all();
		assertTrue(list2.size() == 1);
		assertTrue(list2.get(0).getString("name").equals("it.wikipedia.org"));
	}
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testProcessTopPages() throws ConfigurationException {
		PagesPartialRankingBatchJob job = new PagesPartialRankingBatchJob(configuration);
		job.process(currentTime);
		
		ResultSet resultSet = session.execute("SELECT count(1) FROM pages_partial_rankings");
		assertEquals(490, resultSet.one().getLong("count"));

		resultSet = session.execute("SELECT count(*) FROM pages_partial_rankings WHERE count >= 3 ALLOW FILTERING");
		assertEquals(3, resultSet.one().getLong("count"));

		resultSet = session.execute("SELECT count(*) FROM pages_partial_rankings WHERE count = 2 ALLOW FILTERING");
		assertEquals(14, resultSet.one().getLong("count"));

		resultSet = session.execute("SELECT * FROM pages_partial_rankings WHERE count = 3 AND name = 'Marie Antoinette' ALLOW FILTERING");
		List<Row> list = resultSet.all();
		assertTrue(list.size() == 1);
		assertTrue(list.get(0).getLong("count") == 3L);

		resultSet = session.execute("SELECT * FROM pages_partial_rankings WHERE count = 3 AND name = 'Simone Zaza' ALLOW FILTERING");
		List<Row> list2 = resultSet.all();
		assertTrue(list2.size() == 1);
		assertTrue(list2.get(0).getString("name").equals("Simone Zaza"));
	}

	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testProcessTopContentPages() throws ConfigurationException {
		ContentPagesPartialRankingBatchJob job = new ContentPagesPartialRankingBatchJob(configuration);
		job.process(currentTime);
		
		ResultSet resultSet = session.execute("SELECT count(1) FROM content_pages_partial_rankings");
		assertEquals(385, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM content_pages_partial_rankings WHERE count >= 3 ALLOW FILTERING");
		assertEquals(3, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM content_pages_partial_rankings WHERE count = 2 ALLOW FILTERING");
		assertEquals(11, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT * FROM content_pages_partial_rankings WHERE count = 3 AND name = 'Marie Antoinette' ALLOW FILTERING");
		List<Row> list = resultSet.all();
		assertTrue(list.size() == 1);
		assertTrue(list.get(0).getLong("count") == 3L);
		
		resultSet = session.execute("SELECT * FROM content_pages_partial_rankings WHERE count = 3 AND name = 'Simone Zaza' ALLOW FILTERING");
		List<Row> list2 = resultSet.all();
		assertTrue(list2.size() == 1);
		assertTrue(list2.get(0).getString("name").equals("Simone Zaza"));
	}
	
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testProcessAbsoluteValues() throws ConfigurationException {
		MetricsPartialBatchJob job = new MetricsPartialBatchJob(configuration);
		job.process(currentTime);
		
		long allEdits = session.execute(
				"SELECT value FROM partial_metrics WHERE year = ? AND month = ? AND day = ? AND hour = ? and name = ?",
				2015, 11, 9, 14, "all_edits").one().getLong("value");
		assertEquals(510, allEdits);
		
		long minorEdits = session.execute(
				"SELECT value FROM partial_metrics WHERE year = ? AND month = ? AND day = ? AND hour = ? and name = ?",
				2015, 11, 9, 14, "minor_edits").one().getLong("value");
		assertEquals(154, minorEdits);

		long sumLength = session.execute(
				"SELECT value FROM partial_metrics WHERE year = ? AND month = ? AND day = ? AND hour = ? and name = ?",
				2015, 11, 9, 14, "sum_length").one().getLong("value");
		assertEquals(204896, sumLength);
	}
}
