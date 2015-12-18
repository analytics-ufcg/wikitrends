package br.edu.ufcg.analytics.wikitrends.integration.batch1;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import br.edu.ufcg.analytics.wikitrends.processing.batch1.AbsoluteValuesBatch1;
import br.edu.ufcg.analytics.wikitrends.processing.batch1.TopContentPagesBatch1;
import br.edu.ufcg.analytics.wikitrends.processing.batch1.TopEditorsBatch1;
import br.edu.ufcg.analytics.wikitrends.processing.batch1.TopIdiomsBatch1;
import br.edu.ufcg.analytics.wikitrends.processing.batch1.TopPagesBatch1;
import br.edu.ufcg.analytics.wikitrends.storage.CassandraJobTimesStatusManager;
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

	private static LocalDateTime currentTime;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void prepareMasterDataset() throws Exception {
		System.setProperty("spark.cassandra.connection.host", SEED_NODE);
		System.setProperty("spark.master", "local");
		System.setProperty("spark.app.name", "small-test");

		cleanMasterDataset();

		try(
				Cluster cluster = Cluster.builder().addContactPoints(SEED_NODE).build();
				Session session = cluster.newSession();
				){
			
			new CassandraMasterDatasetManager().dropTables(session);
			new CassandraServingLayer1Manager().dropTables(session);
			
			new CassandraJobTimesStatusManager().dropTables(session);
			
			new CassandraMasterDatasetManager().createTables(session);
			new CassandraServingLayer1Manager().createTables(session);
			
			new CassandraJobTimesStatusManager().createTables(session);

		}

		new CassandraMasterDatasetManager().populate(INPUT_FILE);
		
		setCurrentTime(LocalDateTime.of(2015, 11, 9, 13, 00));
	}
	
	/**
	 * Clean master dataset
	 * @throws java.lang.Exception
	 */
//	@AfterClass
	public static void cleanMasterDataset() throws Exception {

		try (Cluster cluster = Cluster.builder().addContactPoints(SEED_NODE).build();
				Session session = cluster.newSession();) {
			new CassandraMasterDatasetManager().dropTables(session);
			new CassandraServingLayer1Manager().dropTables(session);
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
		session.execute("USE batch_views1");

	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void closeCassandraSession() throws Exception {
		session.close();
		cluster.close();
	}

	private static void setCurrentTime(LocalDateTime cTime) {
		currentTime = cTime;
	}

	public static LocalDateTime getCurrentTime() {
		return currentTime;
	}
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testProcessTopEditors() throws ConfigurationException {
		
		session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
										"top_editors_batch_1", 
										getCurrentTime().getYear(), 
										getCurrentTime().getMonthValue(), 
										getCurrentTime().getDayOfMonth(), 
										getCurrentTime().getHour());
		
		TopEditorsBatch1 job = new TopEditorsBatch1(configuration);
		job.process();
		job.finalizeSparkContext();

		assertEquals(327, session.execute("SELECT count(1) FROM batch_views1.top_editors").one().getLong("count"));
		assertEquals(510, session.execute("SELECT sum(count) as ranking_sum FROM batch_views1.top_editors").one().getLong("ranking_sum"));

		long rankingMax = session.execute("SELECT max(count) as ranking_max FROM batch_views1.top_editors").one().getLong("ranking_max");
		long rankingFirst = session.execute("SELECT count as ranking_max FROM batch_views1.top_editors LIMIT 1").one().getLong("ranking_max");

		assertEquals(31, rankingMax);
		assertEquals(31, rankingFirst);
	}
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testProcessTopIdioms() throws ConfigurationException {
		session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
				"top_idioms_batch_1", 
				getCurrentTime().getYear(), 
				getCurrentTime().getMonthValue(), 
				getCurrentTime().getDayOfMonth(), 
				getCurrentTime().getHour());
		
		TopIdiomsBatch1 job = new TopIdiomsBatch1(configuration);
		job.process();
		job.finalizeSparkContext();
		
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
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testProcessTopPages() throws ConfigurationException {
		session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
				"top_pages_batch_1", 
				getCurrentTime().getYear(), 
				getCurrentTime().getMonthValue(), 
				getCurrentTime().getDayOfMonth(), 
				getCurrentTime().getHour());
		
		TopPagesBatch1 job = new TopPagesBatch1(configuration);
		job.process();
		job.finalizeSparkContext();
		
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
	public void testProcessTopContentPages() throws ConfigurationException {
		session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
				"top_content_pages_batch_1", 
				getCurrentTime().getYear(), 
				getCurrentTime().getMonthValue(), 
				getCurrentTime().getDayOfMonth(), 
				getCurrentTime().getHour());
		
		TopContentPagesBatch1 job = new TopContentPagesBatch1(configuration);
		job.process();
		job.finalizeSparkContext();
		
		ResultSet resultSet = session.execute("SELECT count(1) FROM top_content_pages");
		assertEquals(385, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM top_content_pages WHERE count >= 3 ALLOW FILTERING");
		assertEquals(3, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM top_content_pages WHERE count = 2 ALLOW FILTERING");
		assertEquals(11, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT * FROM top_content_pages WHERE count = 3 AND name = 'Marie Antoinette' ALLOW FILTERING");
		List<Row> list = resultSet.all();
		assertTrue(list.size() == 1);
		assertTrue(list.get(0).getLong("count") == 3L);
		
		resultSet = session.execute("SELECT * FROM top_content_pages WHERE count = 3 AND name = 'Simone Zaza' ALLOW FILTERING");
		List<Row> list2 = resultSet.all();
		assertTrue(list2.size() == 1);
		assertTrue(list2.get(0).getString("name").equals("Simone Zaza"));
	}
	
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testProcessAbsoluteValues() throws ConfigurationException {
		session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
				"abs_values_batch_1", 
				getCurrentTime().getYear(), 
				getCurrentTime().getMonthValue(), 
				getCurrentTime().getDayOfMonth(), 
				getCurrentTime().getHour());
		
		AbsoluteValuesBatch1 job = new AbsoluteValuesBatch1(configuration);
		job.process();
		job.finalizeSparkContext();
		
		ResultSet resultSet = session.execute("SELECT * FROM absolute_values WHERE year = 2015 AND month = 11 AND day = 9 AND hour = 14");
		List<Row> list = resultSet.all();
		
		assertTrue(list.size() == 1);
		Map<String, Long> edits_data = list.get(0).getMap("edits_data", String.class, Long.class);
		assertEquals((long)edits_data.get("all_edits"), (long)510);
		assertEquals((long)edits_data.get("minor_edits"), (long)154);
		assertEquals((long)edits_data.get("average_size"), (long)401);
		
		Set<String> distinct_editors_set = list.get(0).getSet("distinct_editors_set", String.class);
		Set<String> distinct_pages_set = list.get(0).getSet("distinct_pages_set", String.class);
		Set<String> distinct_servers_set = list.get(0).getSet("distinct_servers_set", String.class);
		
		assertEquals(distinct_editors_set.size(), 312);
		assertEquals(distinct_pages_set.size(), 490);
		assertEquals(distinct_servers_set.size(), 45);
		
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
