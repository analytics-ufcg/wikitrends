package br.edu.ufcg.analytics.wikitrends.processing.speed;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import br.edu.ufcg.analytics.wikitrends.WikiTrendsCommands;
import br.edu.ufcg.analytics.wikitrends.WikiTrendsProcess;
import scala.Tuple2;

/**
 * {@link WikiTrendsProcess} implementation when a {@link WikiTrendsCommands#SPEED} is chosen. 
 * 
 * @author Felipe Vieira - felipe29vieira@gmail.com
 */
public class KafkaSpeedLayerJob implements WikiTrendsProcess {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8237571274242642523L;
	private static String HOST = "master";
	private static String PORT = "9000";
	private static String USER = "ubuntu";
	private static String PATH = "speed_java";
	private static String PREFIX = "absolute";
	private static String SUFIX = "tsv";
	
	private String outputPath;

	private transient Configuration configuration;

	/**
	 * Default constructor
	 */
	public KafkaSpeedLayerJob(Configuration configuration) {

		this.configuration = configuration;
		outputPath = String.format("hdfs://%s:%s/user/%s/%s/%s", HOST, PORT, USER, PATH, PREFIX);

	}

	/*@ (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.spark.SparkJob#run()
	 */
	@Override
	public void run() {
		SparkConf conf;
		conf = new SparkConf();
		conf.setAppName(configuration.getString("wikitrends.speed.id"));
		Iterator<String> keys = configuration.getKeys();
		while (keys.hasNext()) {
			String key = keys.next();
			conf.set(key, configuration.getString(key));
		}
		
		CassandraConnector connector = CassandraConnector.apply(conf);

		try(JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(30))){
		    Map<String, Integer> topicMap = new HashMap<String, Integer>();
		    String[] topics = configuration.getString("wikitrends.ingestion.kafka.topics").split(",");
		    for (String topic: topics) {
		      topicMap.put(topic, 3);
		    }

		    JavaPairReceiverInputDStream<String, String> lines =
		            KafkaUtils.createStream(ssc, configuration.getString("wikitrends.ingestion.zookeepeer.servers"), 
		            		configuration.getString("wikitrends.ingestion.kafka.consumergroup"), topicMap);

			JavaPairDStream<String, Integer> allEdits = lines
				.mapToPair(l -> new Tuple2<>("stream_all_edits", 1))
				.reduceByKey((a, b) -> a + b, 1);
			
			JavaPairDStream<String, Integer> minorEdits = lines
				.mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {	
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(Tuple2<String, String> line) throws Exception {
						JsonElement minor = new JsonParser().parse(line._1).getAsJsonObject().get("minor");
						int weight = minor != null && minor.getAsBoolean() ? 1: 0;
						return new Tuple2<>("stream_minor_edits", weight);
					}
	
				})
				.reduceByKey((a, b) -> a + b, 1);
			
			allEdits
			.union(minorEdits)
			.saveAsNewAPIHadoopFiles(outputPath, SUFIX, String.class, Integer.class, TextOutputFormat.class);
			
			allEdits.print();
					    		
			ssc.start();
			ssc.awaitTermination();
		}
	}
}
