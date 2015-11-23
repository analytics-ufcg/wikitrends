/**
 * 
 */
package br.edu.ufcg.analytics.wikitrends.view.api;

import org.apache.commons.configuration.Configuration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import br.edu.ufcg.analytics.wikitrends.WikiTrendsProcess;

/**
 * 
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 */
@SpringBootApplication
public class WikiTrendsRESTServer implements WikiTrendsProcess {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2444360545111891336L;
	
	public WikiTrendsRESTServer() {
		// TODO Auto-generated constructor stub
	}

	public WikiTrendsRESTServer(Configuration configuration) {
		
	}

	@Override
	public void run() {
		SpringApplication.run(WikiTrendsRESTServer.class);
	}

}
