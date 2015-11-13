package br.edu.ufcg.analytics.wikitrends.processing;

import br.edu.ufcg.analytics.wikitrends.processing.batch.BatchLayerJob;
import br.edu.ufcg.analytics.wikitrends.processing.speed.SpeedLayerJob;

/**
 * Possible layers to submit WikiTrends jobs according to &lambda; architecture.
 * 
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 */
public enum LambdaLayer {
	
	BATCH {
		@Override
		public SparkJob buildJob() {
			return new BatchLayerJob();
		}
	},
	SPEED {
		@Override
		public SparkJob buildJob() {
			return new SpeedLayerJob();
		}
	};

	/**
	 * @return A new {@link SparkJob} to execute according to {@link LambdaLayer} instance.
	 */
	public abstract SparkJob buildJob();

}
