package br.edu.ufcg.analytics.wikitrends.processing.batch;

import java.lang.reflect.InvocationTargetException;

import org.apache.commons.configuration.Configuration;

/**
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 *
 */
public class BatchLayerJobBuilder {
	
	/**
	 * @param configuration 
	 * @return 
	 */
	public static BatchLayerJob build(Configuration configuration){
		
		try {
			return (BatchLayerJob) Class.forName(configuration.getString("wikitrends.batch.class")).getConstructor(Configuration.class).newInstance(configuration);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

}
