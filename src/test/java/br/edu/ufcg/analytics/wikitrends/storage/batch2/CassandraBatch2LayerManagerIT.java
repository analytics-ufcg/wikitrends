package br.edu.ufcg.analytics.wikitrends.storage.batch2;

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

import br.edu.ufcg.analytics.wikitrends.storage.serving2.CassandraServingLayer2Manager;

/**
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 * @author Guilherme Gadelha
 *
 */
public class CassandraBatch2LayerManagerIT {
	
	private JavaSparkContext sc;
	private Cluster cluster;
	private Session session;
	private BatchViews2DataGenerator dataGen;
	
	@Before
	public void setup() {
		String seedNode = "localhost";
		
		SparkConf conf = new SparkConf();
        conf.setAppName("Testing Results Layer");
        conf.setMaster("local");
        conf.set("spark.cassandra.connection.host", "localhost");
        
        sc = new JavaSparkContext(conf);
        
        String[] testHosts = seedNode.split(",");
        
        cluster = Cluster.builder().addContactPoints(testHosts).build();
        session = cluster.newSession();
        
        CassandraServingLayer2Manager serving2Manager = new CassandraServingLayer2Manager();
		serving2Manager.dropTables(session);
        serving2Manager.createTables(session);
        
        session.execute("USE results;");
        
        dataGen = new BatchViews2DataGenerator(sc);
	}
	
	@Test
	public void testEmptyTopEditorsTableCreation() {
		ResultSet resultSet = session.execute("SELECT * FROM top_editor;");
		assertTrue(resultSet.all().isEmpty());
	}

	@Test
	public void testEmptyTopPagesTableCreation() {
		ResultSet resultSet = session.execute("SELECT * FROM top_page;");
		assertTrue(resultSet.all().isEmpty());
	}
	
	@Test
	public void testEmptyAbsoluteValuesTableCreation() {
		ResultSet resultSet = session.execute("SELECT * FROM absolute_values;");
		assertTrue(resultSet.all().isEmpty());
	}
	
	@Test
	public void testEmptyTopContentPagesTableCreation() {
		ResultSet resultSet = session.execute("SELECT * FROM top_content_page;");
		assertTrue(resultSet.all().isEmpty());
	}
	
	@Test
	public void testEmptyTopServersTableCreation() {
		ResultSet resultSet = session.execute("SELECT * FROM top_idiom;");
		assertTrue(resultSet.all().isEmpty());
	}
	
	@Test
	public void testCreateTopEditors() {
		dataGen.generateResultingTopEditorsData();
		
		ResultSet resultSet0 = session.execute("SELECT * FROM top_editor;");
		assertEquals(resultSet0.all().size(), 5);
		
		ResultSet resultSet1 = session.execute("SELECT * FROM top_editor LIMIT 3;");
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
		
		ResultSet resultSet0 = session.execute("SELECT * FROM top_idiom;");
		assertEquals(resultSet0.all().size(), 5);
		
		ResultSet resultSet1 = session.execute("SELECT * FROM top_idiom LIMIT 3;");
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
		
		ResultSet resultSet0 = session.execute("SELECT * FROM top_page;");
		assertEquals(resultSet0.all().size(), 5);
		
		ResultSet resultSet1 = session.execute("SELECT * FROM top_page LIMIT 3;");
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
		
		ResultSet resultSet0 = session.execute("SELECT * FROM top_content_page;");
		assertEquals(resultSet0.all().size(), 5);
		
		ResultSet resultSet1 = session.execute("SELECT * FROM top_content_page LIMIT 4;");
		assertEquals(resultSet1.all().size(), 4);
		
		for(Row r : resultSet1) {	
			assertTrue(r.getInt("count") >= 2);
			if(r.getString("content_page").equals("content_page_4")) {
				assertTrue(r.getInt("count") == 3);
			}
		}
	}
	
	
	@Test
	public void testCreateAbsoluteValues() {
		dataGen.generateAbsoluteValuesData();
		
		ResultSet resultSet00 = session.execute("SELECT * FROM absolute_values;");
		assertEquals(resultSet00.all().size(), 6);
		
	}
	
	@After
	public void stop() {
		sc.stop();
		session.close();
		cluster.close();
	}
}
