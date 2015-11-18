package br.edu.ufcg.analytics.wikitrends.processing;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.EnumUtils;

/**
 * Entry point for both batch and speed layer Apache Spark jobs.
 * 
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 */
public class Main {

	private static final String DEFAULT_CONFIG_FILEPATH = "wikitrends.properties";

	/**
	 * Usage: spark-submit --class br.edu.ufcg.analytics.wikitrends.Main JAR_FILE_NAME.jar &lt;seed&gt;
	 * @throws ConfigurationException when a proper configuration file cannot be found. 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ConfigurationException, IOException, InterruptedException, ExecutionException {
		
		if (args.length < 1 || args.length > 2) {
			System.err.println("Usage: java -cp <path-to-jar-files> Main <LAMBDA LAYER> [wikitrends.properties]");
			System.exit(1);
		}
		
		Configuration configuration = new PropertiesConfiguration(args.length == 2? args[1]: DEFAULT_CONFIG_FILEPATH);
		
		if (EnumUtils.isValidEnum(LambdaLayer.class, args[0])) {
			LambdaLayer.valueOf(args[0]).buildJob(configuration).run();
		}
		
		else {
			ExternalServicesEnum.valueOf(args[0]).startService(configuration);
		}
		
	}
}

