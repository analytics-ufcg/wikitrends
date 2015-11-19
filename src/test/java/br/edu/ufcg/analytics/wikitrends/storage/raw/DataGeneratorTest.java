package br.edu.ufcg.analytics.wikitrends.storage.raw;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 *
 */
public class DataGeneratorTest {
	
	private static String seedNode = "localhost";

	@Before
	public void clean(){
		try(Cluster cluster = Cluster.builder().addContactPoints(seedNode).build();){
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
		String inputFile = "src/test/resources/small_test_data.json";
		new DataGenerator(seedNode , inputFile).run();
	}
}
