package br.edu.ufcg.analytics.wikitrends.storage.serving;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 */
public class CreateCassandraSchema {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		String[] nodes = "localhost".split(",");
		
		try(
				Cluster cluster = Cluster.builder().addContactPoints(nodes).build();
				Session session = cluster.newSession();){
			new TablesGenerator(session).generateTables();
		}
	}
}
