package br.edu.ufcg.analytics.wikitrends.processing.speed;


import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import br.edu.ufcg.analytics.wikitrends.WikiTrendsCommands;
import br.edu.ufcg.analytics.wikitrends.WikiTrendsProcess;
import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditChange;
import br.edu.ufcg.analytics.wikitrends.storage.raw.types.LogChange;
import br.edu.ufcg.analytics.wikitrends.storage.raw.types.RawWikimediaChange;
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
	
	private void persistObjects(JavaPairReceiverInputDStream<String, String> lines) {
		JavaDStream<JsonObject> linesAsJsonObjects = lines.
	    		map(l -> new JsonParser().parse(l._2).getAsJsonObject());
		
		JavaDStream<RawWikimediaChange> changesDStream = linesAsJsonObjects.
				map(change -> RawWikimediaChange.parseRawWikimediaChange(change));
		
		JavaDStream<EditChange> editsDStream = linesAsJsonObjects.filter(change -> {
			String type = change.get("type").getAsString();
			return !("log".equals(type));
		}).map(change -> EditChange.parseEditChange(change));
	    
	    JavaDStream<LogChange> logsDStream = linesAsJsonObjects.filter(change -> {
			String type = change.get("type").getAsString();
			return "log".equals(type);
		}).map(change -> LogChange.parseLogChange(change));
	    
	    CassandraStreamingJavaUtil.javaFunctions(changesDStream).
	    	writerBuilder("master_dataset", "change", mapToRow(RawWikimediaChange.class)).
	    	saveToCassandra();
	    
	    CassandraStreamingJavaUtil.javaFunctions(editsDStream).
	    	writerBuilder("master_dataset", "edits", mapToRow(EditChange.class)).
	    	saveToCassandra();
	    
	    CassandraStreamingJavaUtil.javaFunctions(logsDStream).
	    	writerBuilder("master_dataset", "logs", mapToRow(LogChange.class)).
	    	saveToCassandra();
	}

	/*@ (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.spark.SparkJob#run()
	 */
	@Override
	public void run() {
		SparkConf conf;
		conf = new SparkConf();
		conf.setAppName(configuration.getString("wikitrends.speed.id"));
		
		try(JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(30))){
		    Map<String, Integer> topicMap = new HashMap<String, Integer>();
		    String[] topics = configuration.getString("wikitrends.ingestion.kafka.topics").split(",");
		    for (String topic: topics) {
		      topicMap.put(topic, 3);
		    }

		    JavaPairReceiverInputDStream<String, String> streamingData =
		            KafkaUtils.createStream(ssc, configuration.getString("wikitrends.ingestion.zookeepeer.servers"), 
		            		configuration.getString("wikitrends.ingestion.kafka.consumergroup"), topicMap);
		    
		    persistObjects(streamingData);

			JavaPairDStream<String, Integer> allEdits = streamingData
				.mapToPair(l -> new Tuple2<>("stream_all_edits", 1))
				.reduceByKey((a, b) -> a + b, 1);
			
			JavaPairDStream<String, Integer> minorEdits = streamingData
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
