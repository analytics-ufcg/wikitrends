package br.edu.ufcg.analytics.wikitrends.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import br.edu.ufcg.analytics.wikitrends.LambdaLayer;
import br.edu.ufcg.analytics.wikitrends.thrift.WikiMediaChange;
import scala.Tuple2;

/**
 * {@link SparkJob} implementation when a {@link LambdaLayer#BATCH} is chosen. 
 * 
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 */
public class BatchLayerJob implements SparkJob {

	private static BatchLayerOutput PAGES_HEADER = new BatchLayerOutput("page", "count");
	private static BatchLayerOutput IDIOMS_HEADER = new BatchLayerOutput("server", "count");
	private static BatchLayerOutput EDITORS_HEADER = new BatchLayerOutput("user", "count");
	private static BatchLayerOutput ABSOLUTE_HEADER = new BatchLayerOutput("field", "count");

	private static String HOST = "master";
	private static String PORT = "9000";
	private static String USER = "ubuntu";
	private static String PATH = "serving_java";
	private static String PAGES_FILE = "pages";
	private static String CONTENT_PAGES_FILE = PAGES_FILE + "_content";
	private static String IDIOMS_FILE = "idioms";
	private static String EDITORS_FILE = "editors";
	private static String ABSOLUTE_FILE = "absolute";


	/**
	 * 
	 */
	private static final long serialVersionUID = -1348604327884764150L;
	private String outputPath;

	/**
	 * Default constructor
	 */
	public BatchLayerJob() {
		outputPath = String.format("hdfs://%s:%s/user/%s/%s/", HOST, PORT, USER, PATH);
	}

	/*@ (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.spark.SparkJob#run()
	 */
	@Override
	public void run() {

		SparkConf conf = new SparkConf();
		conf.setAppName("wikitrends-batch");

		try(JavaSparkContext sc = new JavaSparkContext(conf);){

			JavaRDD<JsonObject> wikipediaEdits = sc.textFile("/user/ubuntu/dataset/newdata.json")
					.map(l -> new JsonParser().parse(l).getAsJsonObject())
					.filter( edit -> edit.get("server_name").getAsString().endsWith("wikipedia.org"))
					.cache();

			processRanking(sc, wikipediaEdits, "title", PAGES_HEADER, outputPath + PAGES_FILE);

			processContentOnlyRanking(sc, wikipediaEdits, "title", PAGES_HEADER, outputPath + CONTENT_PAGES_FILE);

			processRanking(sc, wikipediaEdits, "server_name", IDIOMS_HEADER, outputPath + IDIOMS_FILE);

			processRanking(sc, wikipediaEdits, "user", EDITORS_HEADER, outputPath + EDITORS_FILE);

			processStatistics(sc, wikipediaEdits, ABSOLUTE_HEADER, outputPath + ABSOLUTE_FILE);
}	
	}

	/**
	 * Processes {@link WikiMediaChange}s currently modeled as {@link JsonObject}s and generates a ranking based on given key.
	 *    
	 * @param sc {@link JavaSparkContext}
	 * @param wikipediaEdits input as a {@link JavaRDD}
	 * @param key ranking key
	 * @param path HDFS output path.
	 */
	private void processRanking(JavaSparkContext sc, JavaRDD<JsonObject> wikipediaEdits, String key, BatchLayerOutput header, String path) {
		JavaRDD<BatchLayerOutput> result = wikipediaEdits
				.mapToPair( edit -> {
					return new Tuple2<>(edit.get(key).getAsString(), 1);
				})
				.reduceByKey( (a,b) -> a+b )
				.mapToPair( edit -> edit.swap() )
				.sortByKey(false)
				.map( edit -> new BatchLayerOutput(edit._2, edit._1.toString()) );

		List<BatchLayerOutput> allPages = result.take(20);
		allPages.add(0, header);

		sc.parallelize(allPages).coalesce(1).saveAsTextFile(path);
	}

	/**
	 * Processes content-only {@link WikiMediaChange}s currently modeled as {@link JsonObject}s and generates a ranking based on given key.
	 *    
	 * @param sc {@link JavaSparkContext}
	 * @param wikipediaEdits input as a {@link JavaRDD}
	 * @param key ranking key
	 * @param path HDFS output path.
	 */
	private void processContentOnlyRanking(JavaSparkContext sc, JavaRDD<JsonObject> wikipediaEdits, String key, BatchLayerOutput header, String path) {
		JavaRDD<JsonObject> filteredEdits = wikipediaEdits
				.filter(edits -> edits.get("namespace").getAsInt() == 0);
		
		processRanking(sc, filteredEdits, key, header, path);
	}
	
	private void processStatistics(JavaSparkContext sc, JavaRDD<JsonObject> wikipediaEdits, BatchLayerOutput header, String path) {
		
		List<BatchLayerOutput> statistics = new ArrayList<>();
		statistics.add(header);
		statistics.add(new BatchLayerOutput("all_edits", countAllEdits(wikipediaEdits)));
		statistics.add(new BatchLayerOutput("minor_edits", countMinorEdits(wikipediaEdits)));
		statistics.add(new BatchLayerOutput("average_size", calcAverageEditLength(wikipediaEdits)));
		statistics.add(new BatchLayerOutput("distinct_pages", distinctPages(wikipediaEdits)));
		statistics.add(new BatchLayerOutput("distinct_editors", distinctEditors(wikipediaEdits)));
		statistics.add(new BatchLayerOutput("distinct_servers", distinctServers(wikipediaEdits)));
		statistics.add(new BatchLayerOutput("origin", getOrigin(wikipediaEdits)));

		sc.parallelize(statistics).coalesce(1).saveAsTextFile(path);
	}

	private long countAllEdits(JavaRDD<JsonObject> wikipediaEdits) {
		return wikipediaEdits.count();
	}

	private long countMinorEdits(JavaRDD<JsonObject> wikipediaEdits) {
		return wikipediaEdits.filter(edit -> {
			JsonElement minor = edit.get("minor");
			return minor != null && minor.getAsBoolean();
		}).count();
	}
	
	private long calcAverageEditLength(JavaRDD<JsonObject> wikipediaEdits) {
		JavaRDD<Long> result = wikipediaEdits.filter(edit -> {
			return edit.get("length") != null;
		}).map( edit -> {
			JsonElement newLength = edit.get("length").getAsJsonObject().get("new");
			JsonElement oldLength = edit.get("length").getAsJsonObject().get("old");
			return (newLength != null && !newLength.isJsonNull()? newLength.getAsLong(): 0) - (oldLength != null && !oldLength.isJsonNull()? oldLength.getAsLong(): 0);
		});
		return result.reduce((a, b) -> a+b) / result.count();
	}
	
	private long distinctPages(JavaRDD<JsonObject> wikipediaEdits) {
		return wikipediaEdits.map(edit -> edit.get("title").getAsString()).distinct().count();
	}

	private long distinctServers(JavaRDD<JsonObject> wikipediaEdits) {
		return wikipediaEdits.map(edit -> edit.get("server_name").getAsString())
				.filter(serverName -> serverName.endsWith("wikipedia.org")).distinct().count();
	}

	private long distinctEditors(JavaRDD<JsonObject> wikipediaEdits) {
		return wikipediaEdits
				.filter(edit -> {
					JsonElement isBot = edit.get("bot");
					return isBot != null && !isBot.getAsBoolean();
				})
				.map(edit -> edit.get("user").getAsString()).distinct().count();
	}

	private long getOrigin(JavaRDD<JsonObject> wikipediaEdits) {
		return wikipediaEdits.map(edit -> edit.get("timestamp").getAsLong()).first();
	}

}
