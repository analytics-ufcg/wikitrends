package br.edu.ufcg.analytics.wikitrends.spark;

import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import br.edu.ufcg.analytics.wikitrends.LambdaLayer;
import scala.Tuple2;

/**
 * {@link SparkJob} implementation when a {@link LambdaLayer#BATCH} is chosen. 
 * 
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 */
public class SpeedLayerJob implements SparkJob {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8237571274242642523L;
	private static String HOST = "hdfs-namenode";
	private static String PORT = "9000";
	private static String USER = "ubuntu";
	private static String PATH = "speed_java";
	private static String PREFIX = "absolute";
	private static String SUFIX = "tsv";


	private String outputPath;

	/**
	 * Default constructor
	 */
	public SpeedLayerJob() {

		outputPath = String.format("hdfs://%s:%s/user/%s/%s/%s", HOST, PORT, USER, PATH, PREFIX);
	}

	/*@ (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.spark.SparkJob#run()
	 */
	@Override
	public void run() {

		SparkConf conf;
		conf = new SparkConf();
		conf.setAppName("wikitrends-speed");

		try(JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(30))){


			JavaReceiverInputDStream<String> edits = ssc.socketTextStream("gabdi", 9999);
						
			JavaPairDStream<String, Integer> allEdits = edits
					.mapToPair(new PairFunction<String, String, Integer>() {

						private static final long serialVersionUID = 1L;

						@Override
						public Tuple2<String, Integer> call(String s) throws Exception {
							return new Tuple2<String, Integer>("stream_all_edits", 1);
						}
					})
					.reduceByKey(new Function2<Integer, Integer, Integer>() {

						private static final long serialVersionUID = 1L;

						@Override
						public Integer call(Integer a, Integer b) throws Exception {
							return a + b;
						}
					}, 1);

			JavaPairDStream<String, Integer> minorEdits = edits
					.filter(new Function<String, Boolean>() {

						private static final long serialVersionUID = 1L;

						@Override
						public Boolean call(String edit) throws Exception {
							return edit.contains("minor\": true");
						}
					})
					.mapToPair(new PairFunction<String, String, Integer>() {

						private static final long serialVersionUID = 1L;

						@Override
						public Tuple2<String, Integer> call(String s) throws Exception {
							return new Tuple2<String, Integer>("stream_minor_edits", 1);
						}
					})
					.reduceByKey(new Function2<Integer, Integer, Integer>() {

						private static final long serialVersionUID = 1L;

						@Override
						public Integer call(Integer a, Integer b) throws Exception {
							return a + b;
						}
					}, 1);
			
			allEdits
			.union(minorEdits)
			.saveAsHadoopFiles(outputPath, SUFIX, String.class, Integer.class, TextOutputFormat.class);

			ssc.start();
			ssc.awaitTermination();
		}
	}	
}
