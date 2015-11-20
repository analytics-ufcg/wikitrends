/**
 * 
 */
package br.edu.ufcg.analytics.wikitrends.view.api;

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
	private static final long serialVersionUID = 2379312688542770552L;

	@Override
	public void run() {
		SpringApplication.run(WikiTrendsRESTServer.class);
	}

}
