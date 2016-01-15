/**
 * 
 */
package br.edu.ufcg.analytics.wikitrends.integration.batch2;

import static org.junit.Assert.assertEquals;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
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

import br.edu.ufcg.analytics.wikitrends.processing.JobStatusID;
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
 * Set of tests the runs the Phase 2 from the Workflow : Batch2.
 * It basically calls the method run() from a job built with
 * a starttime/currenttime took from job_times.status table.
 *
 */
public class BigDataBatch2IT {

	private static final String SEED_NODE = "localhost";
	private static final String INPUT_FILE = "src/test/resources/big_test_data2.json";
	private static final String TEST_CONFIGURATION_FILE = "src/test/resources/big_test_wikitrends.properties";
	private static LocalDateTime currentTime;
	private static LocalDateTime stopTime;

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
			
//			new CassandraMasterDatasetManager().dropTables(session);
			new CassandraServingLayer1Manager().dropTables(session);
			new CassandraServingLayer2Manager().dropTables(session);
			
			new CassandraJobTimesStatusManager().dropTables(session);
			
//			new CassandraMasterDatasetManager().createTables(session);
			new CassandraServingLayer1Manager().createTables(session);
			new CassandraServingLayer2Manager().createTables(session);
			
			new CassandraJobTimesStatusManager().createTables(session);

		}

//		new CassandraMasterDatasetManager().populate(INPUT_FILE);
		
		setCurrentTime(LocalDateTime.of(2015, 11, 7, 14, 00));
		setStopTime(LocalDateTime.of(2015, 11, 7, 18, 00)); // <== correct time considering timestamps-controlled small dataset
//		setStopTime(LocalDateTime.of(2015, 11, 7, 20, 00)); // <== testing different stop time to run process
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
	
	private static void setStopTime(LocalDateTime sTime) {
		stopTime = sTime;
	}

	public static LocalDateTime getStopTime() {
		return stopTime;
	}

	/**
	 * @throws ConfigurationException
	 */
	@Test
	@Ignore
	public void testProcessUsersTotalRanking() throws ConfigurationException {

		TopEditorsBatch1 job = new TopEditorsBatch1(configuration);
		LocalDateTime now = LocalDateTime.of(2015, 11, 7, 11, 00);
		for (int i = 0; i < 7; i++) {
			job.setCurrentTime(now);
			job.process();
			now = now.plusHours(1);
		}
		
		try (Cluster cluster = Cluster.builder().addContactPoints(SEED_NODE).build();
				Session session = cluster.newSession();) {
			
			assertEquals(0, session.execute("SELECT count(1) FROM batch_views.users_ranking where year = ? and month = ? and day = ? and hour = ?", 2015, 11, 7, 11).one().getLong("count"));
			assertEquals(2637, session.execute("SELECT count(1) FROM batch_views.users_ranking where year = ? and month = ? and day = ? and hour = ?", 2015, 11, 7, 12).one().getLong("count"));
			assertEquals(5643, session.execute("SELECT count(1) FROM batch_views.users_ranking where year = ? and month = ? and day = ? and hour = ?", 2015, 11, 7, 13).one().getLong("count"));
			assertEquals(5675, session.execute("SELECT count(1) FROM batch_views.users_ranking where year = ? and month = ? and day = ? and hour = ?", 2015, 11, 7, 14).one().getLong("count"));
			assertEquals(5402, session.execute("SELECT count(1) FROM batch_views.users_ranking where year = ? and month = ? and day = ? and hour = ?", 2015, 11, 7, 15).one().getLong("count"));
			assertEquals(3860, session.execute("SELECT count(1) FROM batch_views.users_ranking where year = ? and month = ? and day = ? and hour = ?", 2015, 11, 7, 16).one().getLong("count"));
			assertEquals(0, session.execute("SELECT count(1) FROM batch_views.users_ranking where year = ? and month = ? and day = ? and hour = ?", 2015, 11, 7, 17).one().getLong("count"));
		}
		
//		try(JavaSparkContext sc = new JavaSparkContext("local", "small-data-batch1-test", conf);){
//			CassandraBatchLayer2Job aggregationJob = new CassandraBatchLayer2Job(configuration);
//			
//			aggregationJob.computeFullRankingFromPartial(sc, "users_ranking");
//		}	
	
	}
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testRunTopIdioms() throws ConfigurationException {
		session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
				JobStatusID.TOP_IDIOMS_BATCH_1.getStatus_id(), 
				getCurrentTime().getYear(), 
				getCurrentTime().getMonthValue(), 
				getCurrentTime().getDayOfMonth(), 
				getCurrentTime().getHour());
		
		TopIdiomsBatch1 job = new TopIdiomsBatch1(configuration);
		job.setStopTime(getStopTime());
		job.run();
		
		session.execute("USE batch_views1");
		ResultSet resultSet = session.execute("SELECT count(1) FROM top_idioms");
		assertEquals(162, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM top_idioms WHERE count >= 3 ALLOW FILTERING");
		assertEquals(107, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM top_idioms WHERE count = 2 ALLOW FILTERING");
		assertEquals(18, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT * FROM top_idioms WHERE count = 560 ALLOW FILTERING");
		List<Row> list = resultSet.all();
		assertEquals(list.size(), 1);
		assertEquals(list.get(0).getString("name"), "en.wikipedia.org");
		assertEquals(list.get(0).getInt("year"), 2015);
		assertEquals(list.get(0).getInt("month"), 11);
		assertEquals(list.get(0).getInt("day"), 7);
		assertEquals(list.get(0).getInt("hour"), 17);
		
		
		session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
				JobStatusID.TOP_IDIOMS_BATCH_2.getStatus_id(), 
				getCurrentTime().getYear(), 
				getCurrentTime().getMonthValue(), 
				getCurrentTime().getDayOfMonth(), 
				getCurrentTime().getHour());
		
		TopIdiomsBatch2 job2 = new TopIdiomsBatch2(configuration);
		job2.setStopTime(getStopTime());
		job2.run();
		
		session.execute("USE batch_views2");
		ResultSet resultSet2 = session.execute("SELECT count(1) FROM top_idioms");
		assertEquals(77, resultSet2.one().getLong("count"));
		
		long rankingMax = session.execute("SELECT max(count) as ranking_max FROM top_idioms").one().getLong("ranking_max");
		long rankingFirst = session.execute("SELECT count as ranking_max FROM top_idioms LIMIT 1").one().getLong("ranking_max");

		assertEquals(1538, rankingMax);
		assertEquals(1538, rankingFirst);
		
		resultSet2 = session.execute("SELECT * FROM top_idioms WHERE id='top_idioms' AND count=1538");
		assertEquals(resultSet2.all().get(0).getString("name"), "en.wikipedia.org");
		
		long rankingMin = session.execute("SELECT MIN(COUNT) AS ranking_min FROM top_idioms").one().getLong("ranking_min");
		assertEquals(1, rankingMin);
	}
	

	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testRunTopPages() throws ConfigurationException {
		session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
				JobStatusID.TOP_PAGES_BATCH_1.getStatus_id(), 
				getCurrentTime().getYear(), 
				getCurrentTime().getMonthValue(), 
				getCurrentTime().getDayOfMonth(), 
				getCurrentTime().getHour());
		
		TopPagesBatch1 job = new TopPagesBatch1(configuration);
		job.setStopTime(getStopTime());
		job.run();
		
		session.execute("USE batch_views1");
		ResultSet resultSet = session.execute("SELECT count(1) FROM top_pages");
		assertEquals(4687, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM top_pages WHERE count >= 3 ALLOW FILTERING");
		assertEquals(137, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM top_pages WHERE count = 1 ALLOW FILTERING");
		assertEquals(4169, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT * FROM top_pages WHERE year=2015 AND month=11 AND day=7 AND hour=15 AND count = 5");
		List<Row> list = resultSet.all();
		assertEquals(list.size(), 1);
		assertEquals(list.get(0).getString("name"), "Liste des planètes mineures (23001-24000)");
		assertEquals(list.get(0).getInt("year"), 2015);
		assertEquals(list.get(0).getInt("month"), 11);
		assertEquals(list.get(0).getInt("day"), 7);
		assertEquals(list.get(0).getInt("hour"), 15);
		
		session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
				JobStatusID.TOP_PAGES_BATCH_2.getStatus_id(), 
				getCurrentTime().getYear(), 
				getCurrentTime().getMonthValue(), 
				getCurrentTime().getDayOfMonth(), 
				getCurrentTime().getHour());
		
		TopPagesBatch2 job2 = new TopPagesBatch2(configuration);
		job2.setStopTime(getStopTime());
		job2.run();
		
		session.execute("USE batch_views2");
		ResultSet resultSet2 = session.execute("SELECT count(1) FROM top_pages");
		assertEquals(4610, resultSet2.one().getLong("count"));
		
		long rankingMax = session.execute("SELECT max(count) as ranking_max FROM top_pages").one().getLong("ranking_max");
		long rankingFirst = session.execute("SELECT count as ranking_max FROM top_pages LIMIT 1").one().getLong("ranking_max");

		assertEquals(32, rankingMax);
		assertEquals(32, rankingFirst);
		
		resultSet2 = session.execute("SELECT * FROM top_pages WHERE id='top_pages' AND count=32");
		assertEquals(resultSet2.all().get(0).getString("name"), "Liste des planètes mineures (29001-30000)");
		
		long rankingMin = session.execute("SELECT MIN(COUNT) AS ranking_min FROM top_pages").one().getLong("ranking_min");
		assertEquals(1, rankingMin);
	}
	
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testRunTopContentPages() throws ConfigurationException {
		session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
				JobStatusID.TOP_CONTENT_PAGES_BATCH_1.getStatus_id(), 
				getCurrentTime().getYear(), 
				getCurrentTime().getMonthValue(), 
				getCurrentTime().getDayOfMonth(), 
				getCurrentTime().getHour());
		
		TopContentPagesBatch1 job = new TopContentPagesBatch1(configuration);
		job.setStopTime(getStopTime());
		job.run();
		
		session.execute("USE batch_views1");
		ResultSet resultSet = session.execute("SELECT count(1) FROM top_content_pages ALLOW FILTERING");
		assertEquals(3714, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM top_content_pages WHERE count >= 3 ALLOW FILTERING");
		assertEquals(105, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM top_content_pages WHERE count = 1 ALLOW FILTERING");
		assertEquals(3299, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT * FROM top_content_pages WHERE year=2015 AND month=11 AND day=7 AND hour=15 AND count = 5");
		List<Row> list = resultSet.all();
		assertEquals(list.size(), 1);
		assertEquals(list.get(0).getString("name"), "Liste des planètes mineures (23001-24000)");
		assertEquals(list.get(0).getInt("year"), 2015);
		assertEquals(list.get(0).getInt("month"), 11);
		assertEquals(list.get(0).getInt("day"), 7);
		assertEquals(list.get(0).getInt("hour"), 15);
		
		session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
				JobStatusID.TOP_CONTENT_PAGES_BATCH_2.getStatus_id(), 
				getCurrentTime().getYear(), 
				getCurrentTime().getMonthValue(), 
				getCurrentTime().getDayOfMonth(), 
				getCurrentTime().getHour());
		
		TopContentPagesBatch2 job2 = new TopContentPagesBatch2(configuration);
		job2.setStopTime(getStopTime());
		job2.run();
		
		session.execute("USE batch_views2");
		ResultSet resultSet2 = session.execute("SELECT count(1) FROM top_content_pages");
		assertEquals(3677, resultSet2.one().getLong("count"));
		
		long rankingMax = session.execute("SELECT max(count) as ranking_max FROM top_content_pages").one().getLong("ranking_max");
		long rankingFirst = session.execute("SELECT count as ranking_max FROM top_content_pages LIMIT 1").one().getLong("ranking_max");

		assertEquals(32, rankingMax);
		assertEquals(32, rankingFirst);
		
		resultSet2 = session.execute("SELECT * FROM top_content_pages WHERE id='top_content_pages' AND count=32");
		assertEquals(resultSet2.all().get(0).getString("name"), "Liste des planètes mineures (29001-30000)");
		
		long rankingMin = session.execute("SELECT MIN(COUNT) AS ranking_min FROM top_content_pages").one().getLong("ranking_min");
		assertEquals(1, rankingMin);
	}
	
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testRunTopEditors() throws ConfigurationException {
		session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
				JobStatusID.TOP_EDITORS_BATCH_1.getStatus_id(), 
				getCurrentTime().getYear(), 
				getCurrentTime().getMonthValue(), 
				getCurrentTime().getDayOfMonth(), 
				getCurrentTime().getHour());
		
		TopEditorsBatch1 job = new TopEditorsBatch1(configuration);
		job.setStopTime(getStopTime());
		job.run();
		
		session.execute("USE batch_views1");
		ResultSet resultSet = session.execute("SELECT count(1) FROM top_editors ALLOW FILTERING");
		assertEquals(2383, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM top_editors WHERE year=2015 AND month=11 AND day=7 AND hour=15 AND count >= 3 ALLOW FILTERING");
		assertEquals(14, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM top_editors WHERE count = 1 ALLOW FILTERING");
		assertEquals(1576, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT * FROM top_editors WHERE year=2015 AND month=11 AND day=7 AND hour=15 AND count = 13");
		List<Row> list = resultSet.all();
		assertEquals(list.size(), 1);
		assertEquals(list.get(0).getString("name"), "MetroBot");
		assertEquals(list.get(0).getInt("year"), 2015);
		assertEquals(list.get(0).getInt("month"), 11);
		assertEquals(list.get(0).getInt("day"), 7);
		assertEquals(list.get(0).getInt("hour"), 15);
		
		session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
				JobStatusID.TOP_EDITORS_BATCH_2.getStatus_id(), 
				getCurrentTime().getYear(), 
				getCurrentTime().getMonthValue(), 
				getCurrentTime().getDayOfMonth(), 
				getCurrentTime().getHour());
		
		TopEditorsBatch2 job2 = new TopEditorsBatch2(configuration);
		job2.setStopTime(getStopTime());
		job2.run();
		
		session.execute("USE batch_views2");
		ResultSet resultSet2 = session.execute("SELECT count(1) FROM top_editors");
		assertEquals(2161, resultSet2.one().getLong("count"));
		
		long rankingMax = session.execute("SELECT max(count) as ranking_max FROM top_editors").one().getLong("ranking_max");
		long rankingFirst = session.execute("SELECT count as ranking_max FROM top_editors LIMIT 1").one().getLong("ranking_max");

		assertEquals(289, rankingMax);
		assertEquals(289, rankingFirst);
		
		resultSet2 = session.execute("SELECT * FROM top_editors WHERE id='top_editors' AND count=289");
		assertEquals(resultSet2.all().get(0).getString("name"), "MetroBot");
		
		long rankingMin = session.execute("SELECT MIN(COUNT) AS ranking_min FROM top_editors").one().getLong("ranking_min");
		assertEquals(1, rankingMin);
	}
	
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testRunAbsoluteValues() throws ConfigurationException {
		session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
				JobStatusID.ABS_VALUES_BATCH_1.getStatus_id(), 
				getCurrentTime().getYear(), 
				getCurrentTime().getMonthValue(), 
				getCurrentTime().getDayOfMonth(), 
				getCurrentTime().getHour());
		
		AbsoluteValuesBatch1 job = new AbsoluteValuesBatch1(configuration);
		job.setStopTime(getStopTime());
		job.run();
		
		session.execute("USE batch_views1");
		ResultSet resultSet0 = session.execute("SELECT * FROM absolute_values");
		assertEquals(3, resultSet0.all().size());
		
		ResultSet resultSet = session.execute("SELECT * FROM absolute_values WHERE year = 2015 AND month = 11 AND day = 7 AND hour = 15");
		List<Row> list = resultSet.all();
		
		assertEquals(list.size(), 1);
		assertEquals((long)list.get(0).getLong("all_edits"), (long)253);
		assertEquals((long)list.get(0).getLong("minor_edits"), (long)80);
		assertEquals((long)list.get(0).getLong("average_size"), (long)327);
		
		Set<String> distinct_editors_set = list.get(0).getSet("distinct_editors_set", String.class);
		Set<String> distinct_pages_set = list.get(0).getSet("distinct_pages_set", String.class);
		Set<String> distinct_servers_set = list.get(0).getSet("distinct_servers_set", String.class);
		
		assertEquals(distinct_editors_set.size(), 161);
		assertEquals(distinct_pages_set.size(), 247);
		assertEquals(distinct_servers_set.size(), 34);
		
		Long smaller_origin = list.get(0).getLong("smaller_origin");
		DateTime date = new DateTime(smaller_origin, DateTimeZone.UTC);
		
		assertEquals(((long)1446910669000L), (long)smaller_origin);
		
		assertEquals(2015, date.getYear());
		assertEquals(11, date.getMonthOfYear());
		assertEquals(7, date.getDayOfMonth());
		assertEquals(15, date.getHourOfDay());
		assertEquals(37, date.getMinuteOfHour());
		assertEquals(49, date.getSecondOfMinute());
		
		session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
				JobStatusID.ABS_VALUES_BATCH_2.getStatus_id(), 
				getCurrentTime().getYear(), 
				getCurrentTime().getMonthValue(), 
				getCurrentTime().getDayOfMonth(), 
				getCurrentTime().getHour());
		
		AbsoluteValuesBatch2 job2 = new AbsoluteValuesBatch2(configuration);
		job2.setStopTime(getStopTime());
		job2.run();
		
		session.execute("USE batch_views2");
		ResultSet resultSet01 = session.execute("SELECT * FROM absolute_values");
		List<Row> list1 = resultSet01.all();
		
		assertEquals(1, list1.size());
		assertEquals((long)list1.get(0).getLong("all_edits"), (long)5462);
		assertEquals((long)list1.get(0).getLong("minor_edits"), (long)1755);
		assertEquals((long)list1.get(0).getLong("average_size"), (long)338);
		
		Integer distinct_editors_count = list1.get(0).getInt("distinct_editors_count");
		Integer distinct_servers_count = list1.get(0).getInt("distinct_servers_count");
		Long distinct_pages_count = list1.get(0).getLong("distinct_pages_count");
		
		assertEquals((int) distinct_editors_count, 2126);
		assertEquals((long) distinct_pages_count, 4610);
		assertEquals((int) distinct_servers_count, 77);
		
		Long smaller_origin2 = list1.get(0).getLong("smaller_origin");
		DateTime date2 = new DateTime(smaller_origin2, DateTimeZone.UTC);
		
		assertEquals(((long)1446910669000L), (long)smaller_origin2);
		
		assertEquals(2015, date2.getYear());
		assertEquals(11, date2.getMonthOfYear());
		assertEquals(7, date2.getDayOfMonth());
		assertEquals(15, date2.getHourOfDay());
		assertEquals(37, date2.getMinuteOfHour());
		assertEquals(49, date2.getSecondOfMinute());
	}

	@Test
	public void testConsecutiveExecutionsOfRunTopIdioms() throws ConfigurationException {

		/* FIRST HOUR */
		TopIdiomsBatch1 job = new TopIdiomsBatch1(configuration);
		job.setCurrentTime(LocalDateTime.of(2015, 11, 7, 15, 00));
		job.process();
		job.finalizeSparkContext();

		session.execute("USE batch_views1");
		ResultSet firstHourBatch1Count = session.execute("SELECT count(1) FROM top_idioms WHERE year = 2015 and month = 11 and day = 7 and hour = 15");
		assertEquals(34, firstHourBatch1Count.one().getLong("count"));
		ResultSet firstHourBatch1Sum = session.execute("SELECT sum(count) as total_edits FROM top_idioms WHERE year = 2015 and month = 11 and day = 7 and hour = 15");
		assertEquals(253, firstHourBatch1Sum.one().getLong("total_edits"));
		
		TopIdiomsBatch2 job2 = new TopIdiomsBatch2(configuration);
		job2.setCurrentTime(LocalDateTime.of(2015, 11, 7, 15, 00));
		job2.process();
		job2.finalizeSparkContext();

		session.execute("USE batch_views2");
		
		ResultSet firstHourBatch2 = session.execute("SELECT count(1) FROM top_idioms");
		assertEquals(34, firstHourBatch2.one().getLong("count"));
		assertEquals(253, session.execute("SELECT sum(count) as total_edits FROM top_idioms").one().getLong("total_edits"));
		
		
		/* SECOND HOUR */
		
		job = new TopIdiomsBatch1(configuration);
		job.setCurrentTime(LocalDateTime.of(2015, 11, 7, 16, 00));
		job.process();
		job.finalizeSparkContext();

		session.execute("USE batch_views1");
		ResultSet secondHourBatch1Count = session.execute("SELECT count(1) FROM top_idioms WHERE year = 2015 and month = 11 and day = 7 and hour = 16");
		assertEquals(63, secondHourBatch1Count.one().getLong("count"));
		ResultSet secondHourBatch1Sum = session.execute("SELECT sum(count) as total_edits FROM top_idioms WHERE year = 2015 and month = 11 and day = 7 and hour = 16");
		assertEquals(3213, secondHourBatch1Sum.one().getLong("total_edits"));

		job2 = new TopIdiomsBatch2(configuration);
		job2.setCurrentTime(LocalDateTime.of(2015, 11, 7, 16, 00));
		job2.process();
		job2.finalizeSparkContext();

		session.execute("USE batch_views2");
		
		ResultSet secondHourBatch2 = session.execute("SELECT count(1) FROM top_idioms");
		assertEquals(65, secondHourBatch2.one().getLong("count"));
		assertEquals(253 + 3213, session.execute("SELECT sum(count) as total_edits FROM top_idioms").one().getLong("total_edits"));
		
	}
}