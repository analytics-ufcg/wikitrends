package br.edu.ufcg.analytics.wikitrends;

import br.edu.ufcg.analytics.wikitrends.spark.BatchLayerJob;
import br.edu.ufcg.analytics.wikitrends.spark.SparkJob;
import br.edu.ufcg.analytics.wikitrends.spark.SpeedLayerJob;

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
