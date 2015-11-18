package br.edu.ufcg.analytics.wikitrends.processing.speed;

import java.lang.reflect.InvocationTargetException;

import org.apache.commons.configuration.Configuration;

import br.edu.ufcg.analytics.wikitrends.processing.batch.BatchLayerJob;

/**
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 *
 */
public class SpeedLayerJobBuilder {
	
	/**
	 * @param configuration 
	 * @return 
	 */
	public static SpeedLayerJob build(Configuration configuration){
		
		try {
			return (SpeedLayerJob) Class.forName(configuration.getString("wikitrends.speed.class")).getConstructor(Configuration.class).newInstance(configuration);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

}
