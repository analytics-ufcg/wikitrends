package br.edu.ufcg.analytics.wikitrends;

import java.lang.reflect.InvocationTargetException;

import org.apache.commons.configuration.Configuration;

/**
 * Possible layers to submit WikiTrends jobs according to &lambda; architecture.
 * 
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 */
public enum WikiTrendsCommands {
	
	BATCH ("wikitrends.batch.class"),
	SERVING ("wikitrends.serving.class"),
	INGESTOR ("wikitrends.ingestion.class"),
	SPEED ("wikitrends.speed.class"),
	WEB ("wikitrends.view.class");
	
	private String property;

	private WikiTrendsCommands(String className) {
		this.property = className;
	}

	/**
	 * @param configuration 
	 * @return A new {@link WikiTrendsProcess} to execute according to {@link WikiTrendsCommands} instance.
	 */
	public WikiTrendsProcess build(Configuration configuration){
		try {
			return (WikiTrendsProcess) Class.forName(configuration.getString(property)).getConstructor(Configuration.class).newInstance(configuration);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
}
