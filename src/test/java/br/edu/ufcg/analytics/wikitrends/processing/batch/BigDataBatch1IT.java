/**
 * 
 */
package br.edu.ufcg.analytics.wikitrends.processing.batch;

import static org.junit.Assert.assertEquals;

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
 * Set of tests the runs the Phase 1 from the Workflow : Batch1.
 * It basically calls the method run() from a job built with
 * a starttime/currenttime took from job_times.status table.
 * 
 */
public class BigDataBatch1IT {

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
		System.setProperty("spark.driver.allowMultipleContexts", "true");
		System.setProperty("spark.cassandra.output.consistency.level", "LOCAL_ONE");

		cleanMasterDataset();

		try(
				Cluster cluster = Cluster.builder().addContactPoints(SEED_NODE).build();
				Session session = cluster.newSession();
				){
			
			new CassandraMasterDatasetManager().createAll(session);
			new CassandraBatchViewsManager().createAll(session);

		}

		new CassandraMasterDatasetManager().populate(INPUT_FILE);
		
		currentTime = LocalDateTime.of(2015, 11, 7, 14, 00);
		stopTime = LocalDateTime.of(2015, 11, 7, 18, 00);
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
	public void testRunTopIdioms() throws ConfigurationException {
//		session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
//				BatchViewID.IDIOMS_PARTIAL_RANKINGS.toString(), 
//				currentTime.getYear(), 
//				currentTime.getMonthValue(), 
//				currentTime.getDayOfMonth(), 
//				currentTime.getHour());
		
		IdiomsPartialRankingBatchJob job = new IdiomsPartialRankingBatchJob(configuration);
		job.run(currentTime.toString(), stopTime.toString());
		
		session.execute("USE batch_views");
		ResultSet resultSet = session.execute("SELECT count(1) FROM idioms_partial_rankings");
		assertEquals(162, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM idioms_partial_rankings WHERE count >= 3 ALLOW FILTERING");
		assertEquals(107, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM idioms_partial_rankings WHERE count = 2 ALLOW FILTERING");
		assertEquals(18, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT * FROM idioms_partial_rankings WHERE count = 560 ALLOW FILTERING");
		List<Row> list = resultSet.all();
		assertEquals(list.size(), 1);
		assertEquals(list.get(0).getString("name"), "en.wikipedia.org");
		assertEquals(list.get(0).getInt("year"), 2015);
		assertEquals(list.get(0).getInt("month"), 11);
		assertEquals(list.get(0).getInt("day"), 7);
		assertEquals(list.get(0).getInt("hour"), 17);
	}
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testRunTopPages() throws ConfigurationException {
//		session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
//				BatchViewID.PAGES_PARTIAL_RANKINGS.toString(), 
//				currentTime.getYear(), 
//				currentTime.getMonthValue(), 
//				currentTime.getDayOfMonth(), 
//				currentTime.getHour());
		
		
		
		PagesPartialRankingBatchJob job = new PagesPartialRankingBatchJob(configuration);
		job.run(currentTime.toString(), stopTime.toString());
		
		session.execute("USE batch_views");
		ResultSet resultSet = session.execute("SELECT count(1) FROM pages_partial_rankings");
		assertEquals(4687, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM pages_partial_rankings WHERE count >= 3 ALLOW FILTERING");
		assertEquals(137, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM pages_partial_rankings WHERE count = 1 ALLOW FILTERING");
		assertEquals(4169, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT * FROM pages_partial_rankings WHERE year=2015 AND month=11 AND day=7 AND hour=15 AND count = 5");
		List<Row> list = resultSet.all();
		assertEquals(list.size(), 1);
		assertEquals(list.get(0).getString("name"), "Liste des planètes mineures (23001-24000)");
		assertEquals(list.get(0).getInt("year"), 2015);
		assertEquals(list.get(0).getInt("month"), 11);
		assertEquals(list.get(0).getInt("day"), 7);
		assertEquals(list.get(0).getInt("hour"), 15);
	}
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testRunTopContentPages() throws ConfigurationException {
//		session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
//				BatchViewID.CONTENT_PAGES_PARTIAL_RANKINGS.toString(), 
//				currentTime.getYear(), 
//				currentTime.getMonthValue(), 
//				currentTime.getDayOfMonth(), 
//				currentTime.getHour());
		
		ContentPagesPartialRankingBatchJob job = new ContentPagesPartialRankingBatchJob(configuration);
		job.run(currentTime.toString(), stopTime.toString());
		
		session.execute("USE batch_views");
		ResultSet resultSet = session.execute("SELECT count(1) FROM content_pages_partial_rankings ALLOW FILTERING");
		assertEquals(3714, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM content_pages_partial_rankings WHERE count >= 3 ALLOW FILTERING");
		assertEquals(105, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM content_pages_partial_rankings WHERE count = 1 ALLOW FILTERING");
		assertEquals(3299, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT * FROM content_pages_partial_rankings WHERE year=2015 AND month=11 AND day=7 AND hour=15 AND count = 5");
		List<Row> list = resultSet.all();
		assertEquals(list.size(), 1);
		assertEquals(list.get(0).getString("name"), "Liste des planètes mineures (23001-24000)");
		assertEquals(list.get(0).getInt("year"), 2015);
		assertEquals(list.get(0).getInt("month"), 11);
		assertEquals(list.get(0).getInt("day"), 7);
		assertEquals(list.get(0).getInt("hour"), 15);
	}
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testRunTopEditors() throws ConfigurationException {
//		session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
//				BatchViewID.EDITORS_PARTIAL_RANKINGS.toString(), 
//				currentTime.getYear(), 
//				currentTime.getMonthValue(), 
//				currentTime.getDayOfMonth(), 
//				currentTime.getHour());
		
		EditorsPartialRankingBatchJob job = new EditorsPartialRankingBatchJob(configuration);
		job.run(currentTime.toString(), stopTime.toString());
		
		session.execute("USE batch_views");
		ResultSet resultSet = session.execute("SELECT count(1) FROM editors_partial_rankings ALLOW FILTERING");
		assertEquals(2383, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM editors_partial_rankings WHERE year=2015 AND month=11 AND day=7 AND hour=15 AND count >= 3 ALLOW FILTERING");
		assertEquals(14, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT count(*) FROM editors_partial_rankings WHERE count = 1 ALLOW FILTERING");
		assertEquals(1576, resultSet.one().getLong("count"));
		
		resultSet = session.execute("SELECT * FROM editors_partial_rankings WHERE year=2015 AND month=11 AND day=7 AND hour=15 AND count = 13");
		List<Row> list = resultSet.all();
		assertEquals(list.size(), 1);
		assertEquals(list.get(0).getString("name"), "MetroBot");
		assertEquals(list.get(0).getInt("year"), 2015);
		assertEquals(list.get(0).getInt("month"), 11);
		assertEquals(list.get(0).getInt("day"), 7);
		assertEquals(list.get(0).getInt("hour"), 15);
	}
	
	
	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testRunAbsoluteValues() throws ConfigurationException {
//		session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
//				BatchViewID.PARTIAL_METRICS.toString(), 
//				currentTime.getYear(), 
//				currentTime.getMonthValue(), 
//				currentTime.getDayOfMonth(), 
//				currentTime.getHour());
		
		MetricsPartialBatchJob job = new MetricsPartialBatchJob(configuration);
		job.run(currentTime.toString(), stopTime.toString());
		
		session.execute("USE batch_views");
		
		long allEdits = session.execute(
				"SELECT value FROM partial_metrics WHERE year = ? AND month = ? AND day = ? AND hour = ? and name = ?",
				2015, 11, 7, 15, "all_edits").one().getLong("value");
		assertEquals(253, allEdits);
		
		long minorEdits = session.execute(
				"SELECT value FROM partial_metrics WHERE year = ? AND month = ? AND day = ? AND hour = ? and name = ?",
				2015, 11, 7, 15, "minor_edits").one().getLong("value");
		assertEquals(80, minorEdits);

		long sumLength = session.execute(
				"SELECT value FROM partial_metrics WHERE year = ? AND month = ? AND day = ? AND hour = ? and name = ?",
				2015, 11, 7, 15, "sum_length").one().getLong("value");
		assertEquals(82835, sumLength);
	}
}