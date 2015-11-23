package br.edu.ufcg.analytics.wikitrends.storage.serving;

import static org.junit.Assert.assertTrue;

import org.junit.Ignore;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

/**
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 * @author Guilherme Gadelha
 *
 */
public class CassandraServingLayerManagerTest {
	
	private static String seedNode = "localhost";

	@Test
	public void testEmptyTopEditorsTableCreation() {
		String[] testHosts = seedNode.split(",");
		try(Cluster cluster = Cluster.builder().addContactPoints(testHosts).build();){
			
			try(Session session = cluster.newSession();){
				new CassandraServingLayerManager().createTables(session);
			}
			
			try(Session session = cluster.newSession();){
				session.execute("USE batch_views;");
				ResultSet resultSet = session.execute("SELECT * FROM top_editors;");
				assertTrue(resultSet.all().isEmpty());
			}
		}
	}

	@Test
	public void testEmptyTopPagesTableCreation() {
		String[] testHosts = seedNode.split(",");
		try(Cluster cluster = Cluster.builder().addContactPoints(testHosts).build();){
			
			try(Session session = cluster.newSession();){
				new CassandraServingLayerManager().createTables(session);
			}
			try(Session session = cluster.newSession();){
				session.execute("USE batch_views;");
				ResultSet resultSet = session.execute("SELECT * FROM top_pages;");
				assertTrue(resultSet.all().isEmpty());
			}
		}
	}
	
	@Test
	public void testEmptyAbsoluteValuesTableCreation() {
		String[] testHosts = seedNode.split(",");
		try(Cluster cluster = Cluster.builder().addContactPoints(testHosts).build();){
			
			try(Session session = cluster.newSession();){
				new CassandraServingLayerManager().createTables(session);
			}
			try(Session session = cluster.newSession();){
				session.execute("USE batch_views;");
				ResultSet resultSet = session.execute("SELECT * FROM absolute_values;");
				assertTrue(resultSet.all().isEmpty());
			}
		}
	}
	
	@Test
	public void testEmptyTopContentPagesTableCreation() {
		String[] testHosts = seedNode.split(",");
		try(Cluster cluster = Cluster.builder().addContactPoints(testHosts).build();){
			
			try(Session session = cluster.newSession();){
				new CassandraServingLayerManager().createTables(session);
			}
			try(Session session = cluster.newSession();){
				session.execute("USE batch_views;");
				ResultSet resultSet = session.execute("SELECT * FROM top_content_pages;");
				assertTrue(resultSet.all().isEmpty());
			}
		}
	}
	
	@Test
	public void testEmptyTopServersTableCreation() {
		String[] testHosts = seedNode.split(",");
		try(Cluster cluster = Cluster.builder().addContactPoints(testHosts).build();){
			
			try(Session session = cluster.newSession();){
				new CassandraServingLayerManager().createTables(session);
			}
			try(Session session = cluster.newSession();){
				session.execute("USE batch_views;");
				ResultSet resultSet = session.execute("SELECT * FROM top_idioms;");
				assertTrue(resultSet.all().isEmpty());
			}
		}
	}
	
	@Test
	public void testPopulateTables() {
		String[] testHosts = seedNode.split(",");
		try(Cluster cluster = Cluster.builder().addContactPoints(testHosts).build();){
			
			try(Session session = cluster.newSession();){
				new CassandraServingLayerManager().createTables(session);
			}
			
			try(Session session = cluster.newSession();){
				session.execute("USE master_dataset;");
				ResultSet resultSet = session.execute("SELECT * FROM edits;");
				assertTrue(resultSet.all().isEmpty());
			}
			
		}
	}

}
