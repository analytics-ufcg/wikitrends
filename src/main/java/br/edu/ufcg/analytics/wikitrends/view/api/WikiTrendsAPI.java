/**
 * 
 */
package br.edu.ufcg.analytics.wikitrends.view.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 */
@SpringBootApplication
public class WikiTrendsAPI {

	/**
	 * WikiTrends API entry point
	 * 
	 * @param args CLI arguments
	 */
	public static void main(String[] args) {
		SpringApplication.run(WikiTrendsAPI.class, args);
	}

}
