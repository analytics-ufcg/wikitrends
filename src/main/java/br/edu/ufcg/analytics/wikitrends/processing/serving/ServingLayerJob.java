package br.edu.ufcg.analytics.wikitrends.processing.serving;

import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import br.edu.ufcg.analytics.wikitrends.WikiTrendsCommands;
import br.edu.ufcg.analytics.wikitrends.WikiTrendsProcess;
import br.edu.ufcg.analytics.wikitrends.storage.results.types.ResultAbsoluteValuesShot;

/**
 * {@link WikiTrendsProcess} implementation when a {@link WikiTrendsCommands#BATCH} is chosen. 
 * 
 * @author Guilherme Gadelha
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 */
public abstract class ServingLayerJob implements WikiTrendsProcess {

	
	/**
	 * SerialVersionUID for ServingLayerJob
	 * 
	 * @since November 26, 2015
	 */
	private static final long serialVersionUID = 176060876537326003L;
	
	protected JavaSparkContext sc;
	protected transient Configuration configuration;

	/**
	 * Default constructor
	 * @param configuration 
	 */
	public ServingLayerJob(Configuration configuration) {
		this.configuration = configuration;
	}

	/*@ (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.spark.SparkJob#run()
	 */
	@Override
	public void run() {

		SparkConf conf = new SparkConf();
		conf.setAppName(configuration.getString("wikitrends.batch.id"));
		
		Iterator<String> keys = configuration.getKeys();
		while (keys.hasNext()) {
			String key = keys.next();
			conf.set(key, configuration.getString(key));
		}

		this.sc = new JavaSparkContext(conf);
		
		saveResultTopEditors(computeMapEditorToCount());
		saveResultTopContentPages(computeMapContentPagesToCount());
		saveResultTopIdioms(computeMapIdiomsToCount());
		saveResultTopPages(computeMapPagesToCount());
		
		saveResultAbsoluteValues(computeResultAbsoluteValues());
		
	}
	
	protected abstract void saveResultTopPages(Map<String, Integer> mapTitleToCount);

	protected abstract void saveResultTopIdioms(Map<String, Integer> mapIdiomToCount);
	
	protected abstract void saveResultTopContentPages(Map<String, Integer> mapContentPageToCount);
	
	protected abstract void saveResultTopEditors(Map<String, Integer> mapEditorToCount);

	protected abstract void saveResultAbsoluteValues(ResultAbsoluteValuesShot resultAbsValuesShot);

	protected abstract ResultAbsoluteValuesShot computeResultAbsoluteValues();

	protected abstract Map<String, Integer> computeMapEditorToCount();

	protected abstract Map<String, Integer> computeMapPagesToCount();

	protected abstract Map<String, Integer> computeMapIdiomsToCount();

	protected abstract Map<String, Integer> computeMapContentPagesToCount();
}
