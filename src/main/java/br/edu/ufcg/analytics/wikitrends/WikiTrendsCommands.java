package br.edu.ufcg.analytics.wikitrends;

import java.lang.reflect.InvocationTargetException;

import org.apache.commons.configuration.Configuration;

import br.edu.ufcg.analytics.wikitrends.ingestion.Ingestor;
import br.edu.ufcg.analytics.wikitrends.processing.speed.KafkaSpeedLayerJob;
import br.edu.ufcg.analytics.wikitrends.view.api.WikiTrendsRESTServer;
import br.edu.ufcg.analytics.wikitrends.processing.batch.ContentPagesFinalRankingBatchJob;
import br.edu.ufcg.analytics.wikitrends.processing.batch.ContentPagesPartialRankingBatchJob;
import br.edu.ufcg.analytics.wikitrends.processing.batch.EditorsFinalRankingBatchJob;
import br.edu.ufcg.analytics.wikitrends.processing.batch.EditorsPartialRankingBatchJob;
import br.edu.ufcg.analytics.wikitrends.processing.batch.MetricsFinalBatchJob;
import br.edu.ufcg.analytics.wikitrends.processing.batch.IdiomsFinalRankingBatchJob;
import br.edu.ufcg.analytics.wikitrends.processing.batch.IdiomsPartialRankingBatchJob;
import br.edu.ufcg.analytics.wikitrends.processing.batch.PagesFinalRankingBatchJob;
import br.edu.ufcg.analytics.wikitrends.processing.batch.PagesPartialRankingBatchJob;
import br.edu.ufcg.analytics.wikitrends.processing.batch.MetricsPartialBatchJob;

/**
 * Possible layers to submit WikiTrends jobs according to &lambda; architecture.
 * 
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 * @author Guilherme Gadelha
 */
public enum WikiTrendsCommands {
	
	IDIOMS_PARTIAL (IdiomsPartialRankingBatchJob.class),
	IDIOMS_FINAL (IdiomsFinalRankingBatchJob.class),

	EDITORS_PARTIAL (EditorsPartialRankingBatchJob.class),
	EDITORS_FINAL (EditorsFinalRankingBatchJob.class),

	PAGES_PARTIAL (PagesPartialRankingBatchJob.class),
	PAGES_FINAL (PagesFinalRankingBatchJob.class),

	CONTENT_PAGES_PARTIAL (ContentPagesPartialRankingBatchJob.class),
	CONTENT_PAGES_FINAL (ContentPagesFinalRankingBatchJob.class),

	METRICS_PARTIAL (MetricsPartialBatchJob.class),
	METRICS_FINAL (MetricsFinalBatchJob.class),

	INGESTOR (Ingestor.class),
	SPEED (KafkaSpeedLayerJob.class),
	WEB (WikiTrendsRESTServer.class);
	
	private Class<? extends WikiTrendsProcess> clazz;

	private WikiTrendsCommands(Class<? extends WikiTrendsProcess> clazz) {
		this.clazz = clazz;
	}

	/**
	 * @param configuration 
	 * @return A new {@link WikiTrendsProcess} to execute according to {@link WikiTrendsCommands} instance.
	 */
	public WikiTrendsProcess build(Configuration configuration){
		try {
			return (WikiTrendsProcess) clazz.getConstructor(Configuration.class).newInstance(configuration);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			throw new RuntimeException(e);
		}
	}
}
