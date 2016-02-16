/**
 * 
 */
package br.edu.ufcg.analytics.wikitrends.view.api;

import org.apache.commons.configuration.Configuration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import br.edu.ufcg.analytics.wikitrends.WikiTrendsConfigurationException;
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
	private static final long serialVersionUID = -7941895582197542600L;
	private Class<?> application;
	
	public WikiTrendsRESTServer() {
		// TODO Auto-generated constructor stub
	}

	public WikiTrendsRESTServer(Configuration configuration) {
		try {
			application = Class.forName(configuration.getString("wikitrends.view.class"));
		} catch (ClassNotFoundException e) {
			throw new WikiTrendsConfigurationException(e);
		}
	}

	@Override
	public void run(String... args) {
		SpringApplication.run(application);
	}

}
