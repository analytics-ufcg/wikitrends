package br.edu.ufcg.analytics.wikitrends;

/**
 * Configuration exception. Triggered before anything starts running
 * 
 * @author Ricardo Araújo Santos - ricoaraujosantos@gmail.com
 *
 */
public class WikiTrendsConfigurationException extends RuntimeException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 119262640343967521L;

	public WikiTrendsConfigurationException(Exception e) {
		super(e);
	}

	public WikiTrendsConfigurationException() {
		super();
	}

	public WikiTrendsConfigurationException(String message) {
		super(message);
	}

	public WikiTrendsConfigurationException(String message, Exception e) {
		super(message, e);
	}

	public WikiTrendsConfigurationException(String message, Exception e, boolean enableSuppression, boolean writableStackTrace) {
		super(message, e, enableSuppression, writableStackTrace);
	}
}
