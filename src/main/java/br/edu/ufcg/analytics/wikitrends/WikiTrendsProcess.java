package br.edu.ufcg.analytics.wikitrends;

import java.io.Serializable;

/**
 * Models a runnable WikiTrends activity.
 * 
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 */
public interface WikiTrendsProcess extends Serializable{

	/**
	 * Triggers the execution of this activity.
	 */
	void run();

}
