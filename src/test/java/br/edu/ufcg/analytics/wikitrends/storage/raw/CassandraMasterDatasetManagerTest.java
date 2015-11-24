package br.edu.ufcg.analytics.wikitrends.storage.raw;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.junit.Ignore;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

/**
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 *
 */
public class CassandraMasterDatasetManagerTest {
	
	private static String seedNode = "localhost";
	private static String inputFile = "src/test/resources/small_test_data.json";

	/**
	 * 
	 */
	@Test
	public void testEmptyEditsTableCreation() {
		String[] testHosts = seedNode.split(",");
		try(Cluster cluster = Cluster.builder().addContactPoints(testHosts).build();){
			
			try(Session session = cluster.newSession();){
				new CassandraMasterDatasetManager().createTables(session);
			}
			
			try(Session session = cluster.newSession();){
				session.execute("USE master_dataset;");
				ResultSet resultSet = session.execute("SELECT * FROM edits;");
				assertTrue(resultSet.all().isEmpty());
			}
			
		}
	}

	/**
	 * 
	 */
	@Test
	@Ignore
	public void testEmptyLogsTableCreation() {
		String[] testHosts = seedNode.split(",");
//		new CreateCassandraSchema().create(testHosts);
		
		try(
				Cluster cluster = Cluster.builder().addContactPoints(testHosts).build();
				Session session = cluster.newSession();){
			session.execute("USE master_dataset;");
			ResultSet resultSet = session.execute("SELECT * FROM logs;");
			assertTrue(resultSet.all().isEmpty());
		}
	}
	
	/**
	 * 
	 */
	@Test
	public void testPopulateEdits() {
		String[] testHosts = seedNode.split(",");
		try(Cluster cluster = Cluster.builder().addContactPoints(testHosts).build();){
			
			CassandraMasterDatasetManager manager = new CassandraMasterDatasetManager();
			
			try(Session session = cluster.newSession();){
				manager.createTables(session);
			}
			
			manager.populateFrom(seedNode, inputFile);
			
			try(Session session = cluster.newSession();){
				session.execute("USE master_dataset;");
				ResultSet resultSet = session.execute("SELECT * FROM edits;");
				assertFalse(resultSet.all().isEmpty());
			}
			
		}
	}

}
