package br.edu.ufcg.analytics.wikitrends;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Entry point for both batch and speed layer Apache Spark jobs.
 * 
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 * @author Guilherme Gadelha
 */
public class WikiTrends {

	private static final String DEFAULT_CONFIG_FILEPATH = "wikitrends.properties";

	/**
	 * Usage: spark-submit --class br.edu.ufcg.analytics.wikitrends.Main JAR_FILE_NAME.jar &lt;seed&gt;
	 * @throws ConfigurationException when a proper configuration file cannot be found. 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ConfigurationException, IOException, InterruptedException, ExecutionException {
		
		if (args.length < 1) {
			System.err.println("Usage: java -Dwikitrends.properties.file=<path-to-properties-file> -cp <path-to-jar-files> WikiTrends <COMMAND> [arguments]");
			System.exit(1);
		}
		
		Configuration configuration = new PropertiesConfiguration(System.getProperty("wikitrends.properties.file", DEFAULT_CONFIG_FILEPATH));
		
		String command = args[0].toUpperCase();
		
		WikiTrendsCommands.valueOf(command).build(configuration).run(Arrays.copyOfRange(args, 1, args.length));
	}
}

