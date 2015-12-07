/**
 * 
 */
package br.edu.ufcg.analytics.wikitrends.integration;

import static org.junit.Assert.assertEquals;

import java.time.LocalDateTime;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
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


	/**
	 * @throws java.lang.Exception
	 */
//	@Before
	public void setUp() throws Exception {
		String[] testHosts = SEED_NODE.split(",");
		try(Cluster cluster = Cluster.builder().addContactPoints(testHosts).build();){

			CassandraMasterDatasetManager manager = new CassandraMasterDatasetManager();
			CassandraServingLayer1Manager serving = new CassandraServingLayer1Manager();
			CassandraServingLayer2Manager results = new CassandraServingLayer2Manager();

			try(Session session = cluster.newSession();){
				manager.dropTables(session);
				manager.createTables(session);
				
				serving.dropTables(session);
				serving.createTables(session);
				
				results.dropTables(session);
				results.createTables(session);
			}


			SparkConf conf = new SparkConf();
			conf.set("spark.cassandra.connection.host", SEED_NODE);

			try (JavaSparkContext sc = new JavaSparkContext("local", "test", conf);) {
				manager.populateFrom(SEED_NODE, INPUT_FILE);
			}
		}
	}

	/**
	 * @throws java.lang.Exception
	 */
//	@After
	public void tearDown() throws Exception {
		String[] testHosts = SEED_NODE.split(",");
		try(Cluster cluster = Cluster.builder().addContactPoints(testHosts).build();
			Session session = cluster.newSession();){

			new CassandraMasterDatasetManager().dropTables(session);
			new CassandraServingLayer1Manager().dropTables(session);
			new CassandraServingLayer2Manager().dropTables(session);
		}
	}

	/**
	 * @throws ConfigurationException
	 */
	@Test
	public void testProcessUsersTotalRanking() throws ConfigurationException {
		Configuration configuration = new PropertiesConfiguration(TEST_CONFIGURATION_FILE);
		TopEditorsBatch1 job = new TopEditorsBatch1(configuration, null);
		
		SparkConf conf = new SparkConf();
		conf.set("spark.cassandra.connection.host", "localhost");
		try(JavaSparkContext sc = new JavaSparkContext("local", "small-data-batch1-test", conf);){
			
			LocalDateTime now = LocalDateTime.of(2015, 11, 7, 11, 00);
			for (int i = 0; i < 7; i++) {
				job.setCurrentTime(now);
				job.process();
				now = now.plusHours(1);
			}
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
		
		try(JavaSparkContext sc = new JavaSparkContext("local", "small-data-batch1-test", conf);){
			CassandraBatchLayer2Job aggregationJob = new CassandraBatchLayer2Job(configuration);
			
			aggregationJob.computeFullRankingFromPartial(sc, "users_ranking");
		}	
	
	}
	
	
}