package br.edu.ufcg.analytics.wikitrends.data;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import br.edu.ufcg.analytics.wikitrends.storage.raw.TablesGenerator;

/**
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 *
 */
public class TablesGeneratorTest {
	
	@After
	public void clean(){
		
	}

	/**
	 * 
	 */
	@Test
	public void testEmptyEditsTableCreation() {
		String[] testHosts = "localhost".split(",");
		try(Cluster cluster = Cluster.builder().addContactPoints(testHosts).build();){
			
			try(Session session = cluster.newSession();){
				new TablesGenerator(session).generate();
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
		String[] testHosts = "localhost".split(",");
//		new CreateCassandraSchema().create(testHosts);
		
		try(
				Cluster cluster = Cluster.builder().addContactPoints(testHosts).build();
				Session session = cluster.newSession();){
			session.execute("USE master_dataset;");
			ResultSet resultSet = session.execute("SELECT * FROM logs;");
			assertTrue(resultSet.all().isEmpty());
		}
	}

}
