package br.edu.ufcg.analytics.wikitrends.integration.batch2;

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
import org.junit.AfterClass;
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
import br.edu.ufcg.analytics.wikitrends.processing.batch2.AbsoluteValuesBatch2;
import br.edu.ufcg.analytics.wikitrends.processing.batch2.TopContentPagesBatch2;
import br.edu.ufcg.analytics.wikitrends.processing.batch2.TopEditorsBatch2;
import br.edu.ufcg.analytics.wikitrends.processing.batch2.TopIdiomsBatch2;
import br.edu.ufcg.analytics.wikitrends.processing.batch2.TopPagesBatch2;
import br.edu.ufcg.analytics.wikitrends.storage.CassandraJobTimesStatusManager;
import br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager;
import br.edu.ufcg.analytics.wikitrends.storage.serving1.CassandraServingLayer1Manager;
import br.edu.ufcg.analytics.wikitrends.storage.serving2.CassandraServingLayer2Manager;

/**
 * @author Guilherme Gadelha
 * @author Ricardo Araújo Santos - ricoaraujosantos@gmail.com
 *
 */
public class SmallDataBatch2IT {

	private static final String TEST_CONFIGURATION_FILE = "src/test/resources/small_test_wikitrends.properties";
	private static final String INPUT_FILE = "src/test/resources/small_test_data.json";
	private static final String SEED_NODE = "localhost";

	private PropertiesConfiguration configuration;
	private Cluster cluster;
	private Session session;

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
			new CassandraServingLayer2Manager().dropTables(session);
			new CassandraJobTimesStatusManager().dropTables(session);
			
			new CassandraMasterDatasetManager().createTables(session);
			new CassandraServingLayer1Manager().createTables(session);
			new CassandraServingLayer2Manager().createTables(session);
			new CassandraJobTimesStatusManager().createTables(session);

		}

		new CassandraMasterDatasetManager().populate(INPUT_FILE);
	}
	
	/**
	 * Clean master dataset
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void cleanMasterDataset() throws Exception {

//		try (Cluster cluster = Cluster.builder().addContactPoints(SEED_NODE).build();
//				Session session = cluster.newSession();) {
//			new CassandraMasterDatasetManager().dropTables(session);
//			new CassandraServingLayer1Manager().dropTables(session);
//			new CassandraServingLayer2Manager().dropTables(session);
//		}

	}


	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void openCassandraSession() throws Exception {

		configuration = new PropertiesConfiguration(TEST_CONFIGURATION_FILE);

		cluster = Cluster.builder().addContactPoints(SEED_NODE).build();
		session = cluster.newSession();
		session.execute("USE batch_views2");

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
		TopEditorsBatch1 job1 = new TopEditorsBatch1(configuration);
		job1.setCurrentTime(LocalDateTime.of(2015, 11, 9, 14, 00));
		job1.process();
		job1.finalizeSparkContext();
		
		TopEditorsBatch2 job2 = new TopEditorsBatch2(configuration);
		job2.setCurrentTime(LocalDateTime.of(2015, 11, 9, 14, 00));
		job2.process();
		job2.finalizeSparkContext();

		assertEquals(327, session.execute("SELECT count(1) FROM top_editors").one().getLong("count"));
		assertEquals(510, session.execute("SELECT sum(count) as ranking_sum FROM top_editors").one().getLong("ranking_sum"));

		long rankingMax = session.execute("SELECT max(count) as ranking_max FROM top_editors").one().getLong("ranking_max");
		long rankingFirst = session.execute("SELECT count as ranking_max FROM top_editors LIMIT 1").one().getLong("ranking_max");

		assertEquals(31, rankingMax);
		assertEquals(31, rankingFirst);
	}
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testProcessTopIdioms() throws ConfigurationException {
		TopIdiomsBatch1 job1 = new TopIdiomsBatch1(configuration);
		job1.setCurrentTime(LocalDateTime.of(2015, 11, 9, 14, 00));
		job1.process();
		job1.finalizeSparkContext();
		
		TopIdiomsBatch2 job2 = new TopIdiomsBatch2(configuration);
		job2.setCurrentTime(LocalDateTime.of(2015, 11, 9, 14, 00));
		job2.process();
		job2.finalizeSparkContext();
		
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
		
		long rankingMax = session.execute("SELECT max(count) as ranking_max FROM top_idioms").one().getLong("ranking_max");
		long rankingFirst = session.execute("SELECT count as ranking_max FROM top_idioms LIMIT 1").one().getLong("ranking_max");

		assertEquals(115, rankingMax);
		assertEquals(115, rankingFirst);
	}
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testProcessTopPages() throws ConfigurationException {
		TopPagesBatch1 job1 = new TopPagesBatch1(configuration);
		job1.setCurrentTime(LocalDateTime.of(2015, 11, 9, 14, 00));
		job1.process();
		job1.finalizeSparkContext();
		
		TopPagesBatch2 job2 = new TopPagesBatch2(configuration);
		job2.setCurrentTime(LocalDateTime.of(2015, 11, 9, 14, 00));
		job2.process();
		job2.finalizeSparkContext();
		
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
		
		long rankingMax = session.execute("SELECT max(count) as ranking_max FROM top_pages").one().getLong("ranking_max");
		long rankingFirst = session.execute("SELECT count as ranking_max FROM top_pages LIMIT 1").one().getLong("ranking_max");

		assertEquals(3, rankingMax);
		assertEquals(3, rankingFirst);
	}

	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testProcessTopContentPages() throws ConfigurationException {
		TopContentPagesBatch1 job1 = new TopContentPagesBatch1(configuration);
		job1.setCurrentTime(LocalDateTime.of(2015, 11, 9, 14, 00));
		job1.process();
		job1.finalizeSparkContext();
		
		TopContentPagesBatch2 job2 = new TopContentPagesBatch2(configuration);
		job2.setCurrentTime(LocalDateTime.of(2015, 11, 9, 14, 00));
		job2.process();
		job2.finalizeSparkContext();
		
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
		
		long rankingMax = session.execute("SELECT max(count) as ranking_max FROM top_content_pages").one().getLong("ranking_max");
		long rankingFirst = session.execute("SELECT count as ranking_max FROM top_content_pages LIMIT 1").one().getLong("ranking_max");

		assertEquals(3, rankingMax);
		assertEquals(3, rankingFirst);
	}
	
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testProcessAbsoluteValues() throws ConfigurationException {
		AbsoluteValuesBatch1 job1 = new AbsoluteValuesBatch1(configuration);
		job1.setCurrentTime(LocalDateTime.of(2015, 11, 9, 14, 00));
		job1.process();
		job1.finalizeSparkContext();
		
		AbsoluteValuesBatch2 job2 = new AbsoluteValuesBatch2(configuration);
		job2.setCurrentTime(LocalDateTime.of(2015, 11, 9, 14, 00));
		job2.process();
		job2.finalizeSparkContext();
		
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
