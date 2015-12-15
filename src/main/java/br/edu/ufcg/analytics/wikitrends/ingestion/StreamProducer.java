package br.edu.ufcg.analytics.wikitrends.ingestion;

import java.util.concurrent.ExecutionException;

/**
 * Producer abstraction.
 * 
 * @author Ricardo Araújo Santos - ricoaraujosantos@gmail.com
 */
public interface StreamProducer {

	/**
	 * Produce new message to whoever is subscribed to this producer.
	 * @param key TODO
	 * @param message new {@link String} message 
	 * 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	void sendMessage(String key, String message);

}