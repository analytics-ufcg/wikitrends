package br.edu.ufcg.analytics.wikitrends.processing.batch;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import br.edu.ufcg.analytics.wikitrends.processing.LambdaLayer;
import br.edu.ufcg.analytics.wikitrends.processing.SparkJob;
import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditType;
import br.edu.ufcg.analytics.wikitrends.storage.serving.types.AbsoluteValueShot2;
import br.edu.ufcg.analytics.wikitrends.storage.serving.types.TopClass2;
import br.edu.ufcg.analytics.wikitrends.thrift.WikiMediaChange;
import scala.Tuple2;

/**
 * {@link SparkJob} implementation when a {@link LambdaLayer#BATCH} is chosen. 
 * 
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 * @author Guilherme Gadelha
 */
public class BatchLayerJob implements SparkJob {

//	private static BatchLayerOutput PAGES_HEADER = new BatchLayerOutput("page", "count");
//	private static BatchLayerOutput IDIOMS_HEADER = new BatchLayerOutput("server", "count");
//	private static BatchLayerOutput EDITORS_HEADER = new BatchLayerOutput("user", "count");
//	private static BatchLayerOutput ABSOLUTE_HEADER = new BatchLayerOutput("field", "count");

//	private static String HOST = "master";
//	private static String PORT = "9000";
//	private static String USER = "ubuntu";
//	private static String PATH = "serving_java";
	
//	private static String PAGES_FILE = "pages";
//	private static String CONTENT_PAGES_FILE = PAGES_FILE + "_content";
//	private static String IDIOMS_FILE = "idioms";
//	private static String EDITORS_FILE = "editors";
//	private static String ABSOLUTE_FILE = "absolute";

	/**
	 * 
	 */
	private static final long serialVersionUID = -1348604327884764150L;
//	private String outputPath;

	/**
	 * Default constructor
	 */
	public BatchLayerJob() {
//		outputPath = String.format("hdfs://%s:%s/user/%s/%s/", HOST, PORT, USER, PATH);
	}

	/*@ (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.spark.SparkJob#run()
	 */
	@Override
	public void run() {

		SparkConf conf = new SparkConf();
		conf.setAppName("wikitrends-batch");
		conf.set("spark.cassandra.connection.host", "localhost");

		try(JavaSparkContext sc = new JavaSparkContext(conf);){

//			JavaRDD<JsonObject> wikipediaEdits = readInput(sc).cache();
			JavaRDD<EditType> wikipediaEdits = readInputFromCassandra(sc).cache();

//			processRanking(sc, wikipediaEdits, "title", PAGES_HEADER, outputPath + PAGES_FILE);
			processTopPagesRanking(sc, wikipediaEdits, "batch_views", "top_pages");

//			processContentOnlyRanking(sc, wikipediaEdits, "title", PAGES_HEADER, outputPath + CONTENT_PAGES_FILE);
			processTopContentPagesRanking(sc, wikipediaEdits, "batch_views", "top_content_pages");

//			processRanking(sc, wikipediaEdits, "server_name", IDIOMS_HEADER, outputPath + IDIOMS_FILE);
			processTopIdiomsRanking(sc, wikipediaEdits, "batch_views", "top_idioms");

//			processRanking(sc, wikipediaEdits, "user", EDITORS_HEADER, outputPath + EDITORS_FILE);
			processTopEditorsRanking(sc, wikipediaEdits, "batch_views", "top_editors");

//			processStatistics(sc, wikipediaEdits, ABSOLUTE_HEADER, outputPath + ABSOLUTE_FILE);
			processStatistics(sc, wikipediaEdits, "batch_views", "absolute_values");
		}	
	}

	private JavaRDD<JsonObject> readInput(JavaSparkContext sc) {
		JavaRDD<JsonObject> wikipediaEdits = sc.textFile("/user/ubuntu/dataset/newdata.json")
				.map(l -> new JsonParser().parse(l).getAsJsonObject())
				.filter( edit -> edit.get("server_name").getAsString().endsWith("wikipedia.org"));
		return wikipediaEdits;
	}

	private JavaRDD<EditType> readInputFromCassandra(JavaSparkContext sc) {

		JavaRDD<EditType> wikipediaEdits = javaFunctions(sc)
				.cassandraTable("master_dataset", "edits", mapRowTo(EditType.class))
				.select("common_event_title", "common_event_bot")
				.filter(edit -> edit.getCommon_server_name().endsWith("wikipedia.org"));

		return wikipediaEdits;
	}

	/**
	 * Processes {@link WikiMediaChange}s currently modeled as {@link JsonObject}s and generates a ranking based on given key.
	 *    
	 * @param sc {@link JavaSparkContext}
	 * @param wikipediaEdits input as a {@link JavaRDD}
	 * @param key ranking key
	 * @param path HDFS output path.
	 */
	private void processTopEditorsRanking(JavaSparkContext sc, JavaRDD<EditType> wikipediaEdits, String keyspace, String table) {
		JavaRDD<BatchLayerOutput<Integer>> resultRDD = wikipediaEdits
				.mapToPair( edit -> {
					return new Tuple2<>(edit.getCommon_event_user(), 1);
				})
				.reduceByKey( (a,b) -> a+b )
				.mapToPair( edit -> edit.swap() )
				.sortByKey(false)
				.map( edit -> new BatchLayerOutput<Integer>(edit._2, edit._1));
		
		List<BatchLayerOutput<Integer>> allPages = resultRDD.take(20); // data map
		
		Map<String, Integer> data = new HashMap<String, Integer>();
		for(BatchLayerOutput<Integer> t  : allPages) {
			data.put(t.getKey(), t.getValue());
		}
		
		List<TopClass2> output = Arrays.asList(new TopClass2(data));
		
		CassandraJavaUtil.javaFunctions(sc.parallelize(output))
			.writerBuilder(keyspace, table, mapToRow(TopClass2.class))
			.saveToCassandra();
		
		//allPages.add(0, header);

		//sc.parallelize(allPages).coalesce(1).saveAsTextFile(path);
	}
	
	private void processTopPagesRanking(JavaSparkContext sc, JavaRDD<EditType> wikipediaEdits, String keyspace, String table) {
		JavaRDD<BatchLayerOutput<Integer>> resultRDD = wikipediaEdits
				.mapToPair( edit -> {
					return new Tuple2<>(edit.getCommon_event_title(), 1);
				})
				.reduceByKey( (a,b) -> a+b )
				.mapToPair( edit -> edit.swap() )
				.sortByKey(false)
				.map( edit -> new BatchLayerOutput<Integer>(edit._2, edit._1));
		
		List<BatchLayerOutput<Integer>> allPages = resultRDD.take(20); // data map
		
		Map<String, Integer> data = new HashMap<String, Integer>();
		for(BatchLayerOutput<Integer> t  : allPages) {
			data.put(t.getKey(), t.getValue());
		}
		
		List<TopClass2> output = Arrays.asList(new TopClass2(data));
		
		CassandraJavaUtil.javaFunctions(sc.parallelize(output))
			.writerBuilder(keyspace, table, mapToRow(TopClass2.class))
			.saveToCassandra();
		
		//allPages.add(0, header);

		//sc.parallelize(allPages).coalesce(1).saveAsTextFile(path);
	}
	
	private void processTopIdiomsRanking(JavaSparkContext sc, JavaRDD<EditType> wikipediaEdits, String keyspace, String table) {
		JavaRDD<BatchLayerOutput<Integer>> resultRDD = wikipediaEdits
				.mapToPair( edit -> {
					return new Tuple2<>(edit.getCommon_server_name(), 1);
				})
				.reduceByKey( (a,b) -> a+b )
				.mapToPair( edit -> edit.swap() )
				.sortByKey(false)
				.map( edit -> new BatchLayerOutput<Integer>(edit._2, edit._1));
		
		List<BatchLayerOutput<Integer>> allPages = resultRDD.take(20); // data map
		
		Map<String, Integer> data = new HashMap<String, Integer>();
		for(BatchLayerOutput<Integer> t  : allPages) {
			data.put(t.getKey(), t.getValue());
		}
		
		List<TopClass2> output = Arrays.asList(new TopClass2(data));
		
		CassandraJavaUtil.javaFunctions(sc.parallelize(output))
			.writerBuilder(keyspace, table, mapToRow(TopClass2.class))
			.saveToCassandra();
		
		//allPages.add(0, header);

		//sc.parallelize(allPages).coalesce(1).saveAsTextFile(path);
	}

	/**
	 * Processes content-only {@link WikiMediaChange}s currently modeled as {@link JsonObject}s and generates a ranking based on given key.
	 *    
	 * @param sc {@link JavaSparkContext}
	 * @param wikipediaEdits input as a {@link JavaRDD}
	 * @param key ranking key
	 * @param path HDFS output path.
	 */
	private void processTopContentPagesRanking(JavaSparkContext sc, JavaRDD<EditType> wikipediaEdits, String keyspace, String table) {
		JavaRDD<EditType> filteredEdits = wikipediaEdits
				.filter(edits -> edits.getCommon_event_namespace() == 0);

//		processRanking(sc, filteredEdits, key, header, path);
		processTopPagesRanking(sc, filteredEdits, "batch_views", "top_content_pages");
	}

	private void processStatistics(JavaSparkContext sc, JavaRDD<EditType> wikipediaEdits, String keyspace, String table) {

//		List<BatchLayerOutput<String>> statistics = new ArrayList<>();
//		//statistics.add(header);
//		statistics.add(new BatchLayerOutput<String>("all_edits", countAllEdits(wikipediaEdits)));
//		statistics.add(new BatchLayerOutput<String>("minor_edits", countMinorEdits(wikipediaEdits)));
//		statistics.add(new BatchLayerOutput<String>("average_size", calcAverageEditLength(wikipediaEdits)));
//		statistics.add(new BatchLayerOutput<String>("distinct_pages", distinctPages(wikipediaEdits)));
//		statistics.add(new BatchLayerOutput<String>("distinct_editors", distinctEditors(wikipediaEdits)));
//		statistics.add(new BatchLayerOutput<String>("distinct_servers", distinctServers(wikipediaEdits)));
//		statistics.add(new BatchLayerOutput<String>("origin", getOrigin(wikipediaEdits)));
		
		Map<String, String> statistics = new HashMap<String, String>();
		statistics.put("all_edits", countAllEdits(wikipediaEdits).toString());
		statistics.put("minor_edits", countMinorEdits(wikipediaEdits).toString());
		statistics.put("average_size", calcAverageEditLength(wikipediaEdits).toString());
		statistics.put("distinct_pages", distinctPages(wikipediaEdits).toString());
		statistics.put("distinct_editors", distinctEditors(wikipediaEdits).toString());
		statistics.put("distinct_servers", distinctServers(wikipediaEdits).toString());
		statistics.put("origin", getOrigin(wikipediaEdits).toString());
		
		List<AbsoluteValueShot2> output = Arrays.asList(new AbsoluteValueShot2(statistics));
		
		CassandraJavaUtil.javaFunctions(sc.parallelize(output))
			.writerBuilder(keyspace, table, mapToRow(AbsoluteValueShot2.class))
			.saveToCassandra();

//		sc.parallelize(statistics).coalesce(1).saveAsTextFile(path);
	}

	private Long countAllEdits(JavaRDD<EditType> wikipediaEdits) {
		return wikipediaEdits.count();
	}

	private Long countMinorEdits(JavaRDD<EditType> wikipediaEdits) {
		return wikipediaEdits.filter(edit -> {
			Boolean minor = edit.getEdit_minor();
			return minor != null && minor;
		}).count();
	}

	private Long calcAverageEditLength(JavaRDD<EditType> wikipediaEdits) {
		JavaRDD<Long> result = wikipediaEdits.filter(edit -> {
			return edit.getEdit_length() != null;
		}).map( edit -> {
			Long newLength = edit.getEdit_length().containsKey("new") ? edit.getEdit_length().get("new") : 0L;
			Long oldLength = edit.getEdit_length().containsKey("old") ? edit.getEdit_length().get("old") : 0L;
			return newLength - oldLength;
		});
		return result.reduce((a, b) -> a+b) / result.count();
	}

	private Long distinctPages(JavaRDD<EditType> wikipediaEdits) {
		return wikipediaEdits.map(edit -> edit.getCommon_event_title()).distinct().count();
	}

	private Long distinctServers(JavaRDD<EditType> wikipediaEdits) {
		return wikipediaEdits.map(edit -> edit.getCommon_server_name())
				.filter(serverName -> serverName.endsWith("wikipedia.org")).distinct().count();
	}

	private Long distinctEditors(JavaRDD<EditType> wikipediaEdits) {
		return wikipediaEdits
				.filter(edit -> {
					Boolean isBot = edit.getCommon_event_bot();
					return isBot != null && isBot;
				})
				.map(edit -> edit.getCommon_event_user()).distinct().count();
	}

	private Long getOrigin(JavaRDD<EditType> wikipediaEdits) {
		return wikipediaEdits.map(edit -> edit.getEvent_time().getTime()).first();
	}

}
