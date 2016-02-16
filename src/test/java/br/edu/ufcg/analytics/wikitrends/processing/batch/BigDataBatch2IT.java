/**
 * 
 */
package br.edu.ufcg.analytics.wikitrends.processing.batch;

import static org.junit.Assert.assertEquals;

import java.time.LocalDateTime;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import br.edu.ufcg.analytics.wikitrends.storage.batchview.CassandraBatchViewsManager;
import br.edu.ufcg.analytics.wikitrends.storage.master.CassandraMasterDatasetManager;

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

	private static final LocalDateTime FIRST_HOUR = LocalDateTime.of(2015, 11, 7, 14, 00);
	private static final LocalDateTime SECOND_HOUR = LocalDateTime.of(2015, 11, 7, 15, 00);
	private static final LocalDateTime THIRD_HOUR = LocalDateTime.of(2015, 11, 7, 16, 00);
	private static final LocalDateTime FOURTH_HOUR = LocalDateTime.of(2015, 11, 7, 17, 00);
	
	private static final String SEED_NODE = "localhost";
	private static final String INPUT_FILE = "src/test/resources/big_test_data2.json";
	private static final String TEST_CONFIGURATION_FILE = "src/test/resources/big_test_wikitrends.properties";

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
	
	@Test
	public void testConsecutiveExecutionsOfRunTopIdioms() throws ConfigurationException {

		/* FIRST HOUR */
		IdiomsPartialRankingBatchJob job = new IdiomsPartialRankingBatchJob(configuration);
		job.run(FIRST_HOUR.toString(), THIRD_HOUR.toString());

		session.execute("USE batch_views");
		
		ResultSet firstHourBatch1Count = session.execute("SELECT count(1) FROM idioms_partial_rankings WHERE year = 2015 and month = 11 and day = 7 and hour = 15");
		assertEquals(34, firstHourBatch1Count.one().getLong("count"));
		ResultSet firstHourBatch1Sum = session.execute("SELECT sum(count) as total_edits FROM idioms_partial_rankings WHERE year = 2015 and month = 11 and day = 7 and hour = 15");
		assertEquals(253, firstHourBatch1Sum.one().getLong("total_edits"));
		
		IdiomsFinalRankingBatchJob job2 = new IdiomsFinalRankingBatchJob(configuration);
		job2.process();

		ResultSet firstHourBatch2 = session.execute("SELECT count(1) FROM final_rankings  WHERE id = ?", "idioms_final_ranking");
		assertEquals(34, firstHourBatch2.one().getLong("count"));
		assertEquals(253, session.execute("SELECT sum(count) as total_edits FROM final_rankings  WHERE id = ?", "idioms_final_ranking").one().getLong("total_edits"));
		assertEquals(34, session.execute("SELECT value FROM final_metrics WHERE id = ? AND name = ?", "final_metrics", "distinct_idioms_count").one().getLong("value"));
		
		
		/* SECOND HOUR */
		job = new IdiomsPartialRankingBatchJob(configuration);
		job.run(THIRD_HOUR.toString(), FOURTH_HOUR.toString());

		session.execute("USE batch_views");
		ResultSet secondHourBatch1Count = session.execute("SELECT count(1) FROM idioms_partial_rankings WHERE year = 2015 and month = 11 and day = 7 and hour = 16");
		assertEquals(63, secondHourBatch1Count.one().getLong("count"));
		ResultSet secondHourBatch1Sum = session.execute("SELECT sum(count) as total_edits FROM idioms_partial_rankings WHERE year = 2015 and month = 11 and day = 7 and hour = 16");
		assertEquals(3213, secondHourBatch1Sum.one().getLong("total_edits"));

		job2 = new IdiomsFinalRankingBatchJob(configuration);
		job2.process();

		session.execute("USE batch_views");
		
		ResultSet secondHourBatch2 = session.execute("SELECT count(1) FROM final_rankings  WHERE id = ?", "idioms_final_ranking");
		assertEquals(65, secondHourBatch2.one().getLong("count"));
		assertEquals(253 + 3213, session.execute("SELECT sum(count) as total_edits FROM final_rankings  WHERE id = ?", "idioms_final_ranking").one().getLong("total_edits"));
		assertEquals(65, session.execute("SELECT value FROM final_metrics WHERE id = ? AND name = ?", "final_metrics", "distinct_idioms_count").one().getLong("value"));
		
	}
	
	
	@Test
	public void testConsecutiveExecutionsOfRunAbsValues() throws ConfigurationException {

		/* FIRST HOUR */
		MetricsPartialBatchJob job = new MetricsPartialBatchJob(configuration);
		job.run(FIRST_HOUR.toString(), THIRD_HOUR.toString());

		session.execute("USE batch_views");
		
		long firstHourAllEdits = session.execute(
				"SELECT value FROM partial_metrics WHERE year = ? AND month = ? AND day = ? AND hour = ? and name = ?",
				2015, 11, 7, 15, "all_edits").one().getLong("value");
		assertEquals(253, firstHourAllEdits);
		
		long firstHourMinorEdits = session.execute(
				"SELECT value FROM partial_metrics WHERE year = ? AND month = ? AND day = ? AND hour = ? and name = ?",
				2015, 11, 7, 15, "minor_edits").one().getLong("value");
		assertEquals(80, firstHourMinorEdits);

		long firstHourSumLength = session.execute(
				"SELECT value FROM partial_metrics WHERE year = ? AND month = ? AND day = ? AND hour = ? and name = ?",
				2015, 11, 7, 15, "sum_length").one().getLong("value");
		assertEquals(82835, firstHourSumLength);

		MetricsFinalBatchJob job2 = new MetricsFinalBatchJob(configuration);
		job2.process();

		session.execute("USE batch_views");
		
		assertEquals(firstHourAllEdits, session.execute("SELECT value FROM final_metrics WHERE id = ? AND name = ?", "final_metrics", "all_edits").one().getLong("value"));
		assertEquals(firstHourMinorEdits, session.execute("SELECT value FROM final_metrics WHERE id = ? AND name = ?", "final_metrics", "minor_edits").one().getLong("value"));
		assertEquals(firstHourSumLength, session.execute("SELECT value FROM final_metrics WHERE id = ? AND name = ?", "final_metrics", "sum_length").one().getLong("value"));

		
		/* SECOND HOUR */
		job = new MetricsPartialBatchJob(configuration);
		job.run(THIRD_HOUR.toString(), FOURTH_HOUR.toString());

		session.execute("USE batch_views");
		long secondHourAllEdits = session.execute(
				"SELECT value FROM partial_metrics WHERE year = ? AND month = ? AND day = ? AND hour = ? and name = ?",
				2015, 11, 7, 16, "all_edits").one().getLong("value");
		assertEquals(3213, secondHourAllEdits);
		
		long secondHourMinorEdits = session.execute(
				"SELECT value FROM partial_metrics WHERE year = ? AND month = ? AND day = ? AND hour = ? and name = ?",
				2015, 11, 7, 16, "minor_edits").one().getLong("value");
		assertEquals(1077, secondHourMinorEdits);

		long secondHourSumLength = session.execute(
				"SELECT value FROM partial_metrics WHERE year = ? AND month = ? AND day = ? AND hour = ? and name = ?",
				2015, 11, 7, 16, "sum_length").one().getLong("value");
		assertEquals(1113245, secondHourSumLength);

		job2 = new MetricsFinalBatchJob(configuration);
		job2.process();

		session.execute("USE batch_views");
		
		assertEquals(firstHourAllEdits + secondHourAllEdits, session.execute("SELECT value FROM final_metrics WHERE id = ? AND name = ?", "final_metrics", "all_edits").one().getLong("value"));
		assertEquals(firstHourMinorEdits + secondHourMinorEdits, session.execute("SELECT value FROM final_metrics WHERE id = ? AND name = ?", "final_metrics", "minor_edits").one().getLong("value"));
		assertEquals(firstHourSumLength + secondHourSumLength, session.execute("SELECT value FROM final_metrics WHERE id = ? AND name = ?", "final_metrics", "sum_length").one().getLong("value"));
	}

}