package br.edu.ufcg.analytics.wikitrends;

/**
 * Entry point for both batch and speed layer Apache Spark jobs.
 * 
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 */
public class Main {

	/**
	 * Usage: spark-submit --class br.edu.ufcg.analytics.wikitrends.Main JAR_FILE_NAME.jar &lt;seed&gt;
	 * @param args {@link Integer} seed
	 * @throws NumberFormatException when a non-integer seed is provided.
	 */
	public static void main(String[] args) {
		LambdaLayer.valueOf(args[0]).buildJob().run();
	}
}

