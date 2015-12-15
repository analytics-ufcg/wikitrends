package br.edu.ufcg.analytics.wikitrends.storage.serving2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 * @author Guilherme Gadelha
 *
 */
public class CassandraBatchLayer2ManagerIT {
	
	private JavaSparkContext sc;
	private Cluster cluster;
	private Session session;
	private BatchViews2DataGenerator dataGen;
	
	private static final String SEED_NODE = "localhost";
	
	@Before
	public void setup() {
		cluster = Cluster.builder().addContactPoints(SEED_NODE).build();
        session = cluster.newSession();
        session.execute("USE batch_views2;");
        
        new CassandraServingLayer2Manager().dropTables(session);
        new CassandraServingLayer2Manager().createTables(session);
        
        SparkConf conf = new SparkConf();
        conf.setAppName("Testing Serving Layer 2");
        conf.setMaster("local");
        conf.set("spark.cassandra.connection.host", "localhost");
        
        sc = new JavaSparkContext(conf);
        
        dataGen = new BatchViews2DataGenerator(sc);
	}
	
	@After
	public void closeCassandraConnection() {
		sc.close();
		session.close();
		cluster.close();
	}
	
	@Test
	public void testEmptyTopEditorsTableCreation() {
		ResultSet resultSet = session.execute("SELECT * FROM top_editors;");
		assertTrue(resultSet.all().isEmpty());
	}

	@Test
	public void testEmptyTopPagesTableCreation() {
		ResultSet resultSet = session.execute("SELECT * FROM top_pages;");
		assertTrue(resultSet.all().isEmpty());
	}
	
	@Test
	public void testEmptyAbsoluteValuesTableCreation() {
		ResultSet resultSet = session.execute("SELECT * FROM absolute_values;");
		assertTrue(resultSet.all().isEmpty());
	}
	
	@Test
	public void testEmptyTopContentPagesTableCreation() {
		ResultSet resultSet = session.execute("SELECT * FROM top_content_pages;");
		assertTrue(resultSet.all().isEmpty());
	}
	
	@Test
	public void testEmptyTopServersTableCreation() {
		ResultSet resultSet = session.execute("SELECT * FROM top_idioms;");
		assertTrue(resultSet.all().isEmpty());
	}
	
	@Test
	public void testCreateAbsoluteValues() {
		dataGen.generateResultingAbsoluteValuesData();
		
		ResultSet resultSet00 = session.execute("SELECT * FROM absolute_values;");
		assertEquals(resultSet00.all().size(), 1);
		
	}
	
	@Test
	public void testCreateTopEditors() {
		dataGen.generateResultingTopEditorsData();
		
		ResultSet resultSet0 = session.execute("SELECT * FROM top_editors;");
		assertEquals(resultSet0.all().size(), 5);
		
		ResultSet resultSet1 = session.execute("SELECT * FROM top_editors LIMIT 3;");
		assertEquals(resultSet1.all().size(), 3);
		
		for(Row r : resultSet1) {	
			assertTrue(r.getInt("count") > 2);
			if(r.getString("editor").equals("john_5")) {
				assertTrue(r.getInt("count") == 10);
			}
		}
	}
	
	@Test
	public void testCreateTopIdioms() {
		dataGen.generateResultingTopIdiomsData();
		
		ResultSet resultSet0 = session.execute("SELECT * FROM top_idioms;");
		assertEquals(resultSet0.all().size(), 5);
		
		ResultSet resultSet1 = session.execute("SELECT * FROM top_idioms LIMIT 3;");
		assertEquals(resultSet1.all().size(), 3);
		
		for(Row r : resultSet1) {	
			assertTrue(r.getInt("count") > 2);
			if(r.getString("idiom").equals("ru")) {
				assertTrue(r.getInt("count") == 10);
			}
		}
	}
	
	@Test
	public void testCreateTopPages() {
		dataGen.generateResultingTopPagesData();
		
		ResultSet resultSet0 = session.execute("SELECT * FROM top_pages;");
		assertEquals(resultSet0.all().size(), 5);
		
		ResultSet resultSet1 = session.execute("SELECT * FROM top_pages LIMIT 3;");
		assertEquals(resultSet1.all().size(), 3);
		
		for(Row r : resultSet1) {	
			assertTrue(r.getInt("count") > 2);
			if(r.getString("page").equals("page_3")) {
				assertTrue(r.getInt("count") == 4);
			}
		}
	}
	
	@Test
	public void testCreateTopContentPages() {
		dataGen.generateResultingTopContentPagesData();
		
		ResultSet resultSet0 = session.execute("SELECT * FROM top_content_pages;");
		assertEquals(resultSet0.all().size(), 5);
		
		ResultSet resultSet1 = session.execute("SELECT * FROM top_content_pages LIMIT 4;");
		assertEquals(resultSet1.all().size(), 4);
		
		for(Row r : resultSet1) {	
			assertTrue(r.getInt("count") >= 2);
			if(r.getString("content_page").equals("content_page_4")) {
				assertTrue(r.getInt("count") == 3);
			}
		}
	}
}
