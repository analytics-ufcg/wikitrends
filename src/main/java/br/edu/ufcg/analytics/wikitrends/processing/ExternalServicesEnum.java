package br.edu.ufcg.analytics.wikitrends.processing;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.commons.configuration.Configuration;

import br.edu.ufcg.analytics.wikitrends.ingestion.Ingestor;

public enum ExternalServicesEnum {
	INGESTOR {
		@Override
		public void startService(Configuration configuration) throws IOException, InterruptedException, ExecutionException {
			Ingestor ingestor = new Ingestor(configuration);
			ingestor.start();
		}
	};
	
	/**
	 * @param configuration 
	 * @return A new {@link SparkJob} to execute according to {@link LambdaLayer} instance.
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public abstract void startService(Configuration configuration) throws IOException, InterruptedException, ExecutionException;
}
