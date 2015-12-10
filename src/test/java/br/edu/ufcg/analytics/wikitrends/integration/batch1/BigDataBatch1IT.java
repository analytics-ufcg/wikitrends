/**
 * 
 */
package br.edu.ufcg.analytics.wikitrends.integration.batch1;

import static org.junit.Assert.assertEquals;

import java.time.LocalDateTime;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import br.edu.ufcg.analytics.wikitrends.processing.batch1.TopEditorsBatch1;
import br.edu.ufcg.analytics.wikitrends.processing.batch2.CassandraBatchLayer2Job;
import br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager;
import br.edu.ufcg.analytics.wikitrends.storage.serving1.CassandraServingLayer1Manager;
import br.edu.ufcg.analytics.wikitrends.storage.serving2.CassandraServingLayer2Manager;

/**
 * @author Ricardo Araújo Santos - ricoaraujosantos@gmail.com
 *
 */
public class BigDataBatch1IT {

	private static final String SEED_NODE = "localhost";
	private static final String INPUT_FILE = "src/test/resources/big_test_data.bigjson";
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

		cleanMasterDataset();

		try(
				Cluster cluster = Cluster.builder().addContactPoints(SEED_NODE).build();
				Session session = cluster.newSession();
				){

			new CassandraMasterDatasetManager().createTables(session);
			new CassandraServingLayer1Manager().createTables(session);
			new CassandraServingLayer2Manager().createTables(session);

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
			new CassandraMasterDatasetManager().dropTables(session);
			new CassandraServingLayer1Manager().dropTables(session);
			new CassandraServingLayer2Manager().dropTables(session);
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
	
	
}