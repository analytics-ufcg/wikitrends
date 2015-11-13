package br.edu.ufcg.analytics.wikitrends.data;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 */
public class CreateCassandraSchema {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		String[] nodes = args[0].split(",");
		
		try(
				Cluster cluster = Cluster.builder().addContactPoints(nodes).build();
				Session session = cluster.newSession();){
			new TablesGenerator(session).generate();
		}
	}
}
