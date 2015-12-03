package br.edu.ufcg.analytics.wikitrends.processing.batch1;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;

import br.edu.ufcg.analytics.wikitrends.WikiTrendsCommands;
import br.edu.ufcg.analytics.wikitrends.WikiTrendsProcess;
import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditType;
import br.edu.ufcg.analytics.wikitrends.storage.serving1.types.TopClass;

/**
 * {@link WikiTrendsProcess} implementation when a {@link WikiTrendsCommands#BATCH} is chosen. 
 * 
 * @author Guilherme Gadelha
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 */
public abstract class BatchLayer1Job implements WikiTrendsProcess {

	private static final long serialVersionUID = 833872580572610849L;

	protected transient Configuration configuration;
	
	private LocalDateTime now;
	private LocalDateTime end;
	private String[] seeds;

	private String batchViewsKeyspace;
	
	/**
	 * Default constructor
	 * @param configuration 
	 */
	public BatchLayer1Job(Configuration configuration) {
		this.configuration = configuration;
		
		setBatchViewsKeyspace(configuration.getString("wikitrends.batch.cassandra.keyspace"));
		
		seeds = configuration.getStringArray("spark.cassandra.connection.host");

		try (Cluster cluster = Cluster.builder().addContactPoints(seeds).build();
				Session session = cluster.newSession();) {
			ResultSet resultSet = session.execute("SELECT * FROM batch_views.status WHERE id = ? LIMIT 1", "servers_ranking");
			List<Row> all = resultSet.all();
			if(!all.isEmpty()){
				Row row = all.get(0);
				now = LocalDateTime.of(row.getInt("year"), row.getInt("month"), row.getInt("day"), row.getInt("hour"), 0).plusHours(1) ;
			}else{
				now = LocalDateTime.ofInstant(Instant.ofEpochMilli(configuration.getLong("wikitrends.batch.incremental.starttime") * 1000), ZoneId.systemDefault());
			}
		}

		//		end = LocalDateTime.ofInstant(Instant.ofEpochMilli((System.currentTimeMillis() / 3600000) * 3600000), ZoneId.systemDefault());
		end = LocalDateTime.ofInstant(Instant.ofEpochMilli(configuration.getLong("wikitrends.batch.incremental.stoptime") * 1000), ZoneId.systemDefault());
		//		end = LocalDateTime.of(2015, 11, 9, 12, 0) ;
	}

	public String getBatchViewsKeyspace() {
		return batchViewsKeyspace;
	}

	public void setBatchViewsKeyspace(String batchViewsKeyspace) {
		this.batchViewsKeyspace = batchViewsKeyspace;
	}

	public LocalDateTime getNow() {
		return now;
	}

	public void setNow(LocalDateTime now) {
		this.now = now;
	}

	public LocalDateTime getEnd() {
		return end;
	}

	public void setEnd(LocalDateTime end) {
		this.end = end;
	}

	public void run(JavaSparkContext sc) {
//		processEditorsRanking(sc);
//		processTopPages(sc);
//		processTopIdioms(sc);
//		processTopContentPages(sc);
		
//		JavaRDD<BatchLayer1Output<Integer>> serverRanking = processRanking(sc, serverRDD);
//		saveServerRanking(sc, serverRanking);
//
//		processStatistics(sc, wikipediaEdits);
	
	}
	
	@Override
	public void run() {

		SparkConf conf = new SparkConf();
		conf.setAppName(configuration.getString("wikitrends.batch.id"));

		Iterator<String> keys = configuration.getKeys();
		while (keys.hasNext()) {
			String key = keys.next();
			conf.set(key, configuration.getString(key));
		}

		try(JavaSparkContext sc = new JavaSparkContext(conf);
				Cluster cluster = Cluster.builder().addContactPoints(seeds).build();
				Session session = cluster.newSession();) {

			new TopEditorsBatch1(configuration).process(sc);
			new TopContentPagesBatch1(configuration).process(sc);
			new TopPagesBatch1(configuration).process(sc);
			new TopIdiomsBatch1(configuration).process(sc);
			new AbsoluteValuesBatch1(configuration).process(sc);

			while(now.isBefore(end)){
				this.run(sc);
				session.execute("INSERT INTO batch_views.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", "servers_ranking", now.getYear(), now.getMonthValue(), now.getDayOfMonth(), now.getHour());
				now = now.plusHours(1);
			}
		}

	}
	
	public JavaRDD<EditType> read(JavaSparkContext sc) {
		JavaRDD<EditType> wikipediaEdits = javaFunctions(sc).cassandraTable("master_dataset", "edits")
				.select("event_time", "common_event_bot", "common_event_title", "common_server_name", "common_event_user",
						"common_event_namespace", "edit_minor", "edit_length")
				.where("year = ? and month = ? and day = ? and hour = ?", now.getYear(), now.getMonthValue(), now.getDayOfMonth(), now.getHour())
				.map(new Function<CassandraRow, EditType>() {
					private static final long serialVersionUID = 1L;

					@Override
					public EditType call(CassandraRow v1) throws Exception {
						EditType edit = new EditType();
						edit.setEvent_time(v1.getDate("event_time"));
						edit.setCommon_event_bot(v1.getBoolean("common_event_bot"));
						edit.setCommon_event_title(v1.getString("common_event_title"));
						edit.setCommon_event_user(v1.getString("common_event_user"));
						edit.setCommon_event_namespace(v1.getInt("common_event_namespace"));
						edit.setCommon_server_name(v1.getString("common_server_name"));
						edit.setEditMinor(v1.getBoolean("edit_minor"));
						edit.setEdit_length(v1.getMap("edit_length", CassandraJavaUtil.typeConverter(String.class), CassandraJavaUtil.typeConverter(Long.class)));
						return edit;
					}

				});
		return wikipediaEdits;
	}
	
	protected JavaRDD<TopClass> processRankingEntry(JavaSparkContext sc, JavaPairRDD<String,Integer> pairRDD) {
		JavaRDD<TopClass> result = pairRDD
				.reduceByKey( (a,b) -> a+b )
				.map( edit -> new TopClass(edit._1, (long) edit._2, now.getYear(), now.getMonthValue(), now.getDayOfMonth(), now.getHour()) );
		
		return result;
	}
	
//	/**
//	 * Processes WikiMedia database changes currently modeled as {@link JsonObject}s and generates a ranking based on given key.
//	 *    
//	 * @param sc {@link JavaSparkContext}
//	 * @param pairRDD input as a {@link JavaRDD}
//	 * @param key ranking key
//	 * @param path HDFS output path.
//	 * @return 
//	 */
//	@Deprecated
//	protected JavaRDD<BatchLayer1Output<Integer>> processRanking(JavaSparkContext sc, JavaPairRDD<String,Integer> pairRDD) {
//		JavaRDD<BatchLayer1Output<Integer>> result = pairRDD
//				.reduceByKey( (a,b) -> a+b )
//				.mapToPair( edit -> edit.swap() )
//				.sortByKey(false)
//				.map( edit -> new BatchLayer1Output<Integer>(edit._2, edit._1) );
//		
//		return result;
//	}
	
	public abstract void process(JavaSparkContext sc);
	
}
