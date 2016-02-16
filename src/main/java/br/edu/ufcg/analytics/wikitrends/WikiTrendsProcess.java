package br.edu.ufcg.analytics.wikitrends;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Models a runnable WikiTrends activity.
 * 
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 */
public interface WikiTrendsProcess extends Serializable {
	
	static final Logger LOGGER = LoggerFactory.getLogger(WikiTrendsProcess.class);

	/**
	 * Triggers the execution of this activity.
	 * @param args 
	 */
	void run(String... args);

}
