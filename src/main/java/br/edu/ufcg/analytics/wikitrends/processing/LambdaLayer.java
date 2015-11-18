package br.edu.ufcg.analytics.wikitrends.processing;

import org.apache.commons.configuration.Configuration;

import br.edu.ufcg.analytics.wikitrends.processing.batch.BatchLayerJob;
import br.edu.ufcg.analytics.wikitrends.processing.batch.BatchLayerJobBuilder;
import br.edu.ufcg.analytics.wikitrends.processing.speed.SpeedLayerJob;

/**
 * Possible layers to submit WikiTrends jobs according to &lambda; architecture.
 * 
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 */
public enum LambdaLayer {
	
	BATCH {
		@Override
		public SparkJob buildJob(Configuration configuration) {
			return BatchLayerJobBuilder.build(configuration);
		}
	},
	SPEED {
		@Override
		public SparkJob buildJob(Configuration configuration) {
			return new SpeedLayerJob();
		}
	};

	/**
	 * @param configuration 
	 * @return A new {@link SparkJob} to execute according to {@link LambdaLayer} instance.
	 */
	public abstract SparkJob buildJob(Configuration configuration);

}
