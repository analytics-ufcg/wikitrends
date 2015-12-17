package br.edu.ufcg.analytics.wikitrends;

import java.lang.reflect.InvocationTargetException;

import org.apache.commons.configuration.Configuration;

/**
 * Possible layers to submit WikiTrends jobs according to &lambda; architecture.
 * 
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 * @author Guilherme Gadelha
 */
public enum WikiTrendsCommands {
	
	TOP_IDIOMS_BATCH_1 ("wikitrends.batch1.top_idioms.class"),
	TOP_EDITORS_BATCH_1 ("wikitrends.batch1.top_editors.class"),
	TOP_PAGES_BATCH_1 ("wikitrends.batch1.top_pages.class"),
	TOP_CONTENT_PAGES_BATCH_1 ("wikitrends.batch1.top_content_pages.class"),
	ABSOLUTE_VALUES_BATCH_1 ("wikitrends.batch1.absolute_values.class"),
	
	TOP_IDIOMS_BATCH_2 ("wikitrends.batch2.top_idioms.class"),
	TOP_EDITORS_BATCH_2 ("wikitrends.batch2.top_editors.class"),
	TOP_PAGES_BATCH_2 ("wikitrends.batch2.top_pages.class"),
	TOP_CONTENT_PAGES_BATCH_2("wikitrends.batch2.top_content_pages.class"),
	ABSOLUTE_VALUES_BATCH_2 ("wikitrends.batch2.absolute_values.class"),
	
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
