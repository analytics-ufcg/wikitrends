package br.edu.ufcg.analytics.wikitrends.storage.batch1;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import br.edu.ufcg.analytics.wikitrends.storage.serving1.CassandraServingLayer1Manager;

/**
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 * @author Guilherme Gadelha
 *
 */
public class CassandraBatchLayer1ManagerIT {
	
	private JavaSparkContext sc;
	private Cluster cluster;
	private Session session;
	private BatchViewsDataGenerator dataGen;
	
	@Before
	public void setup() {
		String seedNode = "localhost";
		
		SparkConf conf = new SparkConf();
        conf.setAppName("Testing Serving Layer");
        conf.setMaster("local");
        conf.set("spark.cassandra.connection.host", "localhost");
        
        sc = new JavaSparkContext(conf);
        
        String[] testHosts = seedNode.split(",");
        
        cluster = Cluster.builder().addContactPoints(testHosts).build();
        session = cluster.newSession();
        
        new CassandraServingLayer1Manager().createTables(session);
        session.execute("USE batch_views;");
        
        dataGen = new BatchViewsDataGenerator(sc);
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
	public void testCreateTopEditors() {
		dataGen.generateTopEditorsData();
		
		ResultSet resultSet0 = session.execute("SELECT * FROM top_editors;");
		assertEquals(resultSet0.all().size(), 20);
		
		ResultSet resultSet1 = session.execute("SELECT * FROM top_editors where year=2013 AND month=4 AND day=3 AND hour=8;");
		List<Row> rows = resultSet1.all();

		assertEquals(rows.size(), 2);
		
		Row r0 = rows.get(0);
		Row r1 = rows.get(1);
		
		assertTrue(r0.getInt("hour") == 8);
		assertTrue(r0.getString("name").equals("john_2"));
		assertTrue(r0.getLong("count") == 0L);
		assertTrue(r1.getString("name").equals("john_3"));
		assertTrue(r1.getLong("count") == 4L);
		
		ResultSet resultSet2 = session.execute("SELECT * FROM top_editors where year=2013 AND month=4 AND day=4 AND hour=7;");
		assertEquals(resultSet2.all().size(), 1);
	}
	
	@Test
	public void testCreateTopIdioms() {
		dataGen.generateTopIdiomsData();
		
		ResultSet resultSet0 = session.execute("SELECT * FROM top_idioms;");
		assertEquals(resultSet0.all().size(), 20);
		
		ResultSet resultSet1 = session.execute("SELECT * FROM top_idioms where year=2013 AND month=4 AND day=3 AND hour=8;");
		List<Row> rows = resultSet1.all();

		assertEquals(rows.size(), 2);
		
		Row r0 = rows.get(0);
		Row r1 = rows.get(1);
		
		assertTrue(r0.getInt("hour") == 8);
		assertTrue(r0.getString("name").equals("de"));
		assertTrue(r0.getLong("count") == 3L);
		assertTrue(r1.getString("name").equals("ru"));
		assertTrue(r1.getLong("count") == 10L);
		
		ResultSet resultSet2 = session.execute("SELECT * FROM top_idioms where year=2013 AND month=4 AND day=4 AND hour=7;");
		assertEquals(resultSet2.all().size(), 1);
		
	}
	
	@Test
	public void testCreateTopPages() {
		dataGen.generateTopPagesData();
		
		ResultSet resultSet0 = session.execute("SELECT * FROM top_pages;");
		assertEquals(resultSet0.all().size(), 20);
		
		ResultSet resultSet1 = session.execute("SELECT * FROM top_pages where year=2013 AND month=4 AND day=3 AND hour=8;");
		List<Row> rows = resultSet1.all();

		assertEquals(rows.size(), 2);
		
		Row r0 = rows.get(0);
		Row r1 = rows.get(1);
		
		assertTrue(r0.getInt("hour") == 8);
		assertTrue(r0.getString("name").equals("page_4"));
		assertTrue(r0.getLong("count") == 3L);
		assertTrue(r1.getString("name").equals("page_5"));
		assertTrue(r1.getLong("count") == 10L);
		
		ResultSet resultSet2 = session.execute("SELECT * FROM top_pages where year=2013 AND month=4 AND day=4 AND hour=7;");
		assertEquals(resultSet2.all().size(), 1);
		
	}
	
	@Test
	public void testCreateTopContentPages() {
		dataGen.generateTopContentPagesData();
		
		ResultSet resultSet0 = session.execute("SELECT * FROM top_content_pages;");
		assertEquals(resultSet0.all().size(), 20);
		
		ResultSet resultSet1 = session.execute("SELECT * FROM top_content_pages where year=2013 AND month=4 AND day=3 AND hour=8;");
		List<Row> rows = resultSet1.all();

		assertEquals(rows.size(), 2);
		
		Row r0 = rows.get(0);
		Row r1 = rows.get(1);
		
		assertTrue(r0.getInt("hour") == 8);
		assertTrue(r0.getString("name").equals("content_page_4"));
		assertTrue(r0.getLong("count") == 3L);
		assertTrue(r1.getString("name").equals("content_page_5"));
		assertTrue(r1.getLong("count") == 10L);
		
		ResultSet resultSet2 = session.execute("SELECT * FROM top_content_pages where year=2013 AND month=4 AND day=4 AND hour=7;");
		assertEquals(resultSet2.all().size(), 1);
		
	}
	
	
	@Test
	public void testCreateAbsoluteValues() {
		dataGen.generateAbsoluteData();
		
		ResultSet resultSet00 = session.execute("SELECT * FROM absolute_values;");
		assertEquals(resultSet00.all().size(), 6);
		
		ResultSet resultSet01 = session.execute("SELECT * FROM absolute_values where year=2014 AND month=5 AND day=23 AND hour=1;");
		assertEquals(resultSet01.all().size(), 1);
		
		ResultSet resultSet02 = session.execute("SELECT * FROM absolute_values where year=2014 AND month=5 AND day=23 AND hour=2;");
		assertEquals(resultSet02.all().size(), 1);
		
		ResultSet resultSet03 = session.execute("SELECT * FROM absolute_values where year=2014 AND month=5 AND day=23 AND hour=3;");
		assertEquals(resultSet03.all().size(), 1);
		
		for(Row r : resultSet00) {
			Set<String> dServers = r.getSet("distinct_servers", String.class);
			Set<String> dPages = r.getSet("distinct_pages", String.class);
			Set<String> dEditors = r.getSet("distinct_editors", String.class);
			
			assertTrue(dServers.size() == 6 && dPages.size() == 6 && dEditors.size() == 6);
			
			if(r.getInt("hour") == 2) {
				assertTrue(dServers.contains(new String("server1")));
				assertTrue(dPages.contains("page1"));
				assertTrue(dEditors.contains("ed1"));
			}
		}
		
		ResultSet resultSet2 = session.execute("SELECT * FROM absolute_values where year=2014 AND month=05 AND day=23 AND hour=06;");
		assertEquals(resultSet2.all().size(), 1);
	}
	
	@After
	public void stop() {
		sc.stop();
		session.close();
		cluster.close();
	}
}
