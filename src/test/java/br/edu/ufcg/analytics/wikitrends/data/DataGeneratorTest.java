package br.edu.ufcg.analytics.wikitrends.data;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import br.edu.ufcg.analytics.wikitrends.storage.raw.DataGenerator;
import br.edu.ufcg.analytics.wikitrends.storage.raw.TablesGenerator;

/**
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 *
 */
public class DataGeneratorTest {
	
	private static String[] testHosts = "localhost".split(",");

	@Before
	public void clean(){
		try(Cluster cluster = Cluster.builder().addContactPoints(testHosts).build();){
			try(Session session = cluster.newSession();){
				new TablesGenerator(session).generate();
			}
		}
	}

	/**
	 * 
	 */
	@Test
	public void testEmptyEditsTableCreation() {
		String inputFile = "src/test/resources";
		
		try(Cluster cluster = Cluster.builder().addContactPoints(testHosts).build();){
			try(Session session = cluster.newSession();){
				new DataGenerator(inputFile).run();
			}
		}
		
	}
}
