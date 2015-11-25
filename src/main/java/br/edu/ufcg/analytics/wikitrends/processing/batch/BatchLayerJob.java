package br.edu.ufcg.analytics.wikitrends.processing.batch;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.gson.JsonObject;

import br.edu.ufcg.analytics.wikitrends.WikiTrendsCommands;
import br.edu.ufcg.analytics.wikitrends.WikiTrendsProcess;
import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditType;
import scala.Tuple2;

/**
 * {@link WikiTrendsProcess} implementation when a {@link WikiTrendsCommands#BATCH} is chosen. 
 * 
 * @author Guilherme Gadelha
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 */
public abstract class BatchLayerJob implements WikiTrendsProcess {

	/**
	 * 
	 */
	private static final long serialVersionUID = 833872580572610849L;
	protected transient Configuration configuration;

	/**
	 * Default constructor
	 * @param configuration 
	 */
	public BatchLayerJob(Configuration configuration) {
		this.configuration = configuration;
	}

	/*@ (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.spark.SparkJob#run()
	 */
	@Override
	public void run() {

		SparkConf conf = new SparkConf();
		conf.setAppName(configuration.getString("wikitrends.batch.id"));
		
		Iterator<String> keys = configuration.getKeys();
		while (keys.hasNext()) {
			String key = keys.next();
			conf.set(key, configuration.getString(key));
		}

		try(JavaSparkContext sc = new JavaSparkContext(conf);){

			JavaRDD<EditType> wikipediaEdits = readRDD(sc)
					.filter(edit -> edit.getCommon_server_name().endsWith("wikipedia.org"))
					.cache();
			
//			JavaPairRDD<String, Integer> titleRDD = wikipediaEdits
//			.mapPartitionsToPair( iterator -> {
//				ArrayList<Tuple2<String, Integer>> pairs = new ArrayList<>();
//				while(iterator.hasNext()){
//					EditType edit = iterator.next();
//					pairs.add(new Tuple2<String, Integer>(edit.getCommon_event_title(), 1));
//				}
//				return pairs;
//			});
//			JavaRDD<BatchLayerOutput<Integer>> titleRanking = processRanking(sc, titleRDD);
//			
//			saveTitleRanking(sc, titleRanking);
//
//			JavaPairRDD<String, Integer> contentTitleRDD = wikipediaEdits
//			.filter(edits -> "0".equals(edits.getCommon_event_namespace()))
//			.mapPartitionsToPair( iterator -> {
//				ArrayList<Tuple2<String, Integer>> pairs = new ArrayList<>();
//				while(iterator.hasNext()){
//					EditType edit = iterator.next();
//					pairs.add(new Tuple2<String, Integer>(edit.getCommon_event_title(), 1));
//				}
//				return pairs;
//			});
//			JavaRDD<BatchLayerOutput<Integer>> contentTitleRanking = processRanking(sc, contentTitleRDD);
//			
//			saveContentTitleRanking(sc, contentTitleRanking);

			JavaPairRDD<String, Integer> serverRDD = wikipediaEdits
			.mapPartitionsToPair( iterator -> {
				ArrayList<Tuple2<String, Integer>> pairs = new ArrayList<>();
				while(iterator.hasNext()){
					EditType edit = iterator.next();
					pairs.add(new Tuple2<String, Integer>(edit.getCommon_server_name(), 1));
				}
				return pairs;
			});
			JavaRDD<BatchLayerOutput<Integer>> serverRanking = processRanking(sc, serverRDD);
			saveServerRanking(sc, serverRanking);
			
//			JavaPairRDD<String, Integer> userRDD = wikipediaEdits
//			.mapPartitionsToPair( iterator -> {
//				ArrayList<Tuple2<String, Integer>> pairs = new ArrayList<>();
//				while(iterator.hasNext()){
//					EditType edit = iterator.next();
//					pairs.add(new Tuple2<String, Integer>(edit.getCommon_event_user(), 1));
//				}
//				return pairs;
//			});
//			JavaRDD<BatchLayerOutput<Integer>> userRanking = processRanking(sc, userRDD);
//			saveUserRanking(sc, userRanking);
//
//			processStatistics(sc, wikipediaEdits);
		}	
	}
	


	protected abstract JavaRDD<EditType> readRDD(JavaSparkContext sc);

	/**
	 * Processes WikiMedia database changes currently modeled as {@link JsonObject}s and generates a ranking based on given key.
	 *    
	 * @param sc {@link JavaSparkContext}
	 * @param pairRDD input as a {@link JavaRDD}
	 * @param key ranking key
	 * @param path HDFS output path.
	 * @return 
	 */
	private JavaRDD<BatchLayerOutput<Integer>> processRanking(JavaSparkContext sc, JavaPairRDD<String,Integer> pairRDD) {
		JavaRDD<BatchLayerOutput<Integer>> result = pairRDD
				.reduceByKey( (a,b) -> a+b )
				.mapToPair( edit -> edit.swap() )
				.sortByKey(false)
				.map( edit -> new BatchLayerOutput<Integer>(edit._2, edit._1) );
		
		return result;
	}

	protected abstract void saveTitleRanking(JavaSparkContext sc, JavaRDD<BatchLayerOutput<Integer>> titleRanking);

	protected abstract void saveContentTitleRanking(JavaSparkContext sc, JavaRDD<BatchLayerOutput<Integer>> contentTitleRanking);

	protected abstract void saveServerRanking(JavaSparkContext sc, JavaRDD<BatchLayerOutput<Integer>> serverRanking);
	
	protected abstract void saveUserRanking(JavaSparkContext sc, JavaRDD<BatchLayerOutput<Integer>> userRanking);

	protected abstract void processStatistics(JavaSparkContext sc, JavaRDD<EditType> wikipediaEdits);

	protected Long countAllEdits(JavaRDD<EditType> wikipediaEdits) {
		return wikipediaEdits.count();
	}

	protected Long countMinorEdits(JavaRDD<EditType> wikipediaEdits) {
		return wikipediaEdits.filter(edit -> {
			return edit.getEditMinor() != null && edit.getEditMinor();
		}).count();
	}

	protected Long calcAverageEditLength(JavaRDD<EditType> wikipediaEdits) {
		JavaRDD<Long> result = wikipediaEdits.map( edit -> {
			Map<String, Long> length = edit.getEdit_length();
			long oldLength = length.containsKey("old")? length.get("old"): 0;
			long newLength = length.containsKey("new")? length.get("new"): 0;
			return newLength - oldLength;
		});
		return result.reduce((a, b) -> a+b) / result.count();
	}

	protected Set<String> distinctPages(JavaRDD<EditType> wikipediaEdits) {
		return new HashSet<String>(wikipediaEdits.map(edit -> edit.getCommon_event_title()).distinct().collect());
	}

	protected Set<String> distinctServers(JavaRDD<EditType> wikipediaEdits) {
		return new HashSet<String>(wikipediaEdits.map(edit -> edit.getCommon_server_name()).distinct().collect());
	}

	protected Set<String> distinctEditors(JavaRDD<EditType> wikipediaEdits) {
		return new HashSet<String>(
				wikipediaEdits.filter(edit -> {
					return edit.getCommon_event_bot() != null && !edit.getCommon_event_bot();
				})
				.map(edit -> edit.getCommon_event_user()).distinct().collect());
	}

	protected Long getOrigin(JavaRDD<EditType> wikipediaEdits) {
		return wikipediaEdits.first().getEvent_time().getTime();
	}
	
}
