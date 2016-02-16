package br.edu.ufcg.analytics.wikitrends.processing.batch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.LocalDateTime;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
 * Set of tests the runs the process of Phase 2 from the Workflow : Batch2.
 * It basically calls the method process() from a job built with
 * a starttime/currenttime set on the test.
 * 
 * TODO This test is too naive and does not test anything that is already being tested by {@link BigDataBatch2IT}. 
 * FIXME Delete it or update tables to a green bar.
 *
 */
public class SmallDataBatch2IT {

	private static final String TEST_CONFIGURATION_FILE = "src/test/resources/small_test_wikitrends.properties";
	private static final String INPUT_FILE = "src/test/resources/small_test_data.json";
	private static final String SEED_NODE = "localhost";

	private PropertiesConfiguration configuration;
	private Cluster cluster;
	private Session session;

	private static LocalDateTime currentTime;
	private static LocalDateTime stopTime;

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
		stopTime = LocalDateTime.of(2015, 11, 9, 15, 00);
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
	@Ignore
	public void testProcessTopEditors() throws ConfigurationException {
		EditorsPartialRankingBatchJob job1 = new EditorsPartialRankingBatchJob(configuration);
		job1.run(currentTime.toString(), currentTime.plusHours(1).toString());
		
		EditorsFinalRankingBatchJob job2 = new EditorsFinalRankingBatchJob(configuration);
		job2.process();

		assertEquals(327, session.execute("SELECT count(1) FROM final_rankings  where id = ?", "top_editors").one().getLong("count"));
		assertEquals(510, session.execute("SELECT sum(count) as count_sum FROM final_rankings  WHERE id = ?", "top_editors").one().getLong("count_sum"));

		long rankingMax = session.execute("SELECT max(count) as ranking_max FROM final_rankings  WHERE id = ?", "top_editors").one().getLong("ranking_max");
		long rankingFirst = session.execute("SELECT count as ranking_max FROM final_rankings  WHERE id = ? LIMIT 1", "top_editors").one().getLong("ranking_max");

		assertEquals(31, rankingMax);
		assertEquals(31, rankingFirst);
	}
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	@Ignore
	public void testProcessTopIdioms() throws ConfigurationException {
		IdiomsPartialRankingBatchJob job1 = new IdiomsPartialRankingBatchJob(configuration);
		job1.run(currentTime.toString(), stopTime.toString());
		
		IdiomsFinalRankingBatchJob job2 = new IdiomsFinalRankingBatchJob(configuration);
		job2.process();
		
		ResultSet resultSet = session.execute("SELECT count(1) FROM final_rankings  where id = ?", "idioms_final_ranking");
		assertEquals(45, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(1) FROM final_rankings  WHERE id = ? AND count >= ?", "idioms_final_ranking", 3);
		assertEquals(19, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(1) FROM final_rankings  WHERE id = ? AND count = ? ALLOW FILTERING", "idioms_final_ranking", 2);
		assertEquals(12, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT * FROM idioms_partial_rankings WHERE count = 115 AND name = 'en.wikipedia.org' ALLOW FILTERING");
		List<Row> list = resultSet.all();
		assertTrue(list.size() == 1);
		assertTrue(list.get(0).getLong("count") == 115L);
		
		resultSet = session.execute("SELECT * FROM idioms_partial_rankings WHERE count = 55 AND name = 'it.wikipedia.org' ALLOW FILTERING");
		List<Row> list2 = resultSet.all();
		assertTrue(list2.size() == 1);
		assertTrue(list2.get(0).getString("name").equals("it.wikipedia.org"));
		
		long rankingMax = session.execute("SELECT max(count) as ranking_max FROM top_idioms").one().getLong("ranking_max");
		long rankingFirst = session.execute("SELECT count as ranking_max FROM idioms_partial_rankings LIMIT 1").one().getLong("ranking_max");

		assertEquals(115, rankingMax);
		assertEquals(115, rankingFirst);
	}
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	@Ignore
	public void testProcessTopPages() throws ConfigurationException {
		PagesPartialRankingBatchJob job1 = new PagesPartialRankingBatchJob(configuration);
		job1.run(currentTime.toString(), stopTime.toString());
		
		PagesFinalRankingBatchJob job2 = new PagesFinalRankingBatchJob(configuration);
		job2.process();
		
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
		
		long rankingMax = session.execute("SELECT max(count) as ranking_max FROM pages_partial_rankings").one().getLong("ranking_max");
		long rankingFirst = session.execute("SELECT count as ranking_max FROM pages_partial_rankings LIMIT 1").one().getLong("ranking_max");

		assertEquals(3, rankingMax);
		assertEquals(3, rankingFirst);
	}

	/**
	 * @throws ConfigurationException
	 */
	@Test
	@Ignore
	public void testProcessTopContentPages() throws ConfigurationException {
		ContentPagesPartialRankingBatchJob job1 = new ContentPagesPartialRankingBatchJob(configuration);
		job1.run(currentTime.toString(), stopTime.toString());
		
		ContentPagesFinalRankingBatchJob job2 = new ContentPagesFinalRankingBatchJob(configuration);
		job2.process();
		
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
		
		long rankingMax = session.execute("SELECT max(count) as ranking_max FROM content_pages_partial_rankings").one().getLong("ranking_max");
		long rankingFirst = session.execute("SELECT count as ranking_max FROM content_pages_partial_rankings LIMIT 1").one().getLong("ranking_max");

		assertEquals(3, rankingMax);
		assertEquals(3, rankingFirst);
	}
	
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	@Ignore
	public void testProcessAbsoluteValues() throws ConfigurationException {
		MetricsPartialBatchJob job1 = new MetricsPartialBatchJob(configuration);
		job1.run(currentTime.toString(), stopTime.toString());
	
		MetricsFinalBatchJob job2 = new MetricsFinalBatchJob(configuration);
		job2.process();
		
		ResultSet resultSet = session.execute("SELECT * FROM absolute_values");
		List<Row> list = resultSet.all();
		
		assertTrue(list.size() == 1);
		assertEquals((long)list.get(0).getLong(("all_edits")), (long)510);
		assertEquals((long)list.get(0).getLong(("minor_edits")), (long)154);
		assertEquals((long)list.get(0).getLong(("average_size")), (long)401);
		
		Integer distinct_editors_count = list.get(0).getInt("distinct_editors_count");		
		Integer distinct_servers_count = list.get(0).getInt("distinct_servers_count");
		Long distinct_pages_count = list.get(0).getLong("distinct_pages_count");
		
		assertEquals((int) distinct_editors_count, 312);		
		assertEquals((int) distinct_servers_count, 45);
		assertEquals((long) distinct_pages_count, 490);
		
		Long smaller_origin = list.get(0).getLong("smaller_origin");
		DateTime date = new DateTime(smaller_origin);
		
		assertEquals(((long)1447078806000L), (long)smaller_origin);
		
		assertEquals(9, date.getDayOfMonth());
		assertEquals(11, date.getMonthOfYear());
		assertEquals(2015, date.getYear());
		assertEquals(11, date.getHourOfDay());
		assertEquals(20, date.getMinuteOfHour());
		assertEquals(6, date.getSecondOfMinute());
	}
}
