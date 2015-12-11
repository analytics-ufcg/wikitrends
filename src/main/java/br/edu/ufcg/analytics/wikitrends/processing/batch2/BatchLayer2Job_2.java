package br.edu.ufcg.analytics.wikitrends.processing.batch2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowToTuple;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.types.TypeConverter;

import br.edu.ufcg.analytics.wikitrends.WikiTrendsCommands;
import br.edu.ufcg.analytics.wikitrends.WikiTrendsProcess;
import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.TopResult;
import scala.Tuple2;

/**
 * {@link WikiTrendsProcess} implementation when a {@link WikiTrendsCommands#BATCH} is chosen. 
 * 
 * @author Guilherme Gadelha
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 */
public abstract class BatchLayer2Job_2 implements WikiTrendsProcess {

	private static final long serialVersionUID = 1218454132437246895L;
	
	protected transient JavaSparkContext sc;
	protected transient Configuration configuration;
	
	private static LocalDateTime currentTime;
	private LocalDateTime stopTime;
	
	private String[] seeds;
	
	private String batchViews2Keyspace;

	private LocalDateTime startTime;
	
	public BatchLayer2Job_2(Configuration configuration) {
		createJavaSparkContext(configuration);
		
		setBatchViews2Keyspace(configuration.getString("wikitrends.serving.cassandra.keyspace"));
		
		seeds = configuration.getStringArray("spark.cassandra.connection.host");

		try (Cluster cluster = Cluster.builder().addContactPoints(seeds).build();
				Session session = cluster.newSession();) {
			ResultSet resultSet = session.execute("SELECT * FROM batch_views.status WHERE id = ? LIMIT 1", "servers_ranking");
			List<Row> all = resultSet.all();
			if(!all.isEmpty()){
				Row row = all.get(0);
				setCurrentTime(LocalDateTime.of(row.getInt("year"), row.getInt("month"), row.getInt("day"), row.getInt("hour"), 0).plusHours(1));
			}else{
				setCurrentTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(configuration.getLong("wikitrends.batch.incremental.starttime") * 1000), ZoneId.systemDefault()));
			}
		}

		//	end = LocalDateTime.ofInstant(Instant.ofEpochMilli((System.currentTimeMillis() / 3600000) * 3600000), ZoneId.systemDefault());
		setStopTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(configuration.getLong("wikitrends.batch.incremental.stoptime") * 1000), ZoneId.systemDefault()));
		//	end = LocalDateTime.of(2015, 11, 9, 12, 0) ;
	}
	
	public LocalDateTime getStopTime() {
		return this.stopTime;
	}
	
	public void setStopTime(LocalDateTime stopTime) {
		this.stopTime = stopTime;
	}
	
	public LocalDateTime getCurrentTime() {
		return this.currentTime;
	}
	
	public void setCurrentTime(LocalDateTime currentTime) {
		this.currentTime = currentTime;
	}
	
	public LocalDateTime getStartTime() {
		return this.startTime;
	}
	
	public void setStartTime(LocalDateTime startTime) {
		this.startTime = startTime;
	}

	public String getBatchViews2Keyspace() {
		return batchViews2Keyspace;
	}

	public void setBatchViews2Keyspace(String batchViews2Keyspace) {
		this.batchViews2Keyspace = batchViews2Keyspace;
	}
	
	public void createJavaSparkContext(Configuration configuration) {
		SparkConf conf = new SparkConf();
		String appName = configuration.getString("wikitrends.job.batch2.id");
		String master_host = configuration.getString("spark.master.host");
		
		Iterator<String> keys = configuration.getKeys();
		while (keys.hasNext()) {
			String key = keys.next();
			conf.set(key, configuration.getString(key));
		}
		sc = new JavaSparkContext(master_host, appName, conf);
	}
	
	public JavaSparkContext getJavaSparkContext() {
		return this.sc;
	}
	
	/*@ (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.spark.SparkJob#run()
	 */
	@Override
	public void run() {

		SparkConf conf = new SparkConf();
		conf.setAppName(configuration.getString("wikitrends.job.batch2.id"));
		
		Iterator<String> keys = configuration.getKeys();
		while (keys.hasNext()) {
			String key = keys.next();
			conf.set(key, configuration.getString(key));
		}

		this.sc = new JavaSparkContext(conf);
		
		CassandraRow lastBatchExecutionStatus = CassandraJavaUtil.javaFunctions(sc).cassandraTable("batch_views", "status")
				.select("id", "year", "month", "day", "hour")
				.limit(1L).collect().get(0);
		
		try (Cluster cluster = Cluster.builder().addContactPoints(seeds).build();
				Session session = cluster.newSession();) {

			while(getCurrentTime().isBefore(getStopTime())) {
//				setStartTime(LocalDateTime.of(lastBatchExecutionStatus.getInt("year"),
//											  lastBatchExecutionStatus.getInt("month"),
//											  lastBatchExecutionStatus.getInt("day"),
//											  lastBatchExecutionStatus.getInt("hour"),
//											  0));
				
				new TopEditorsBatch2(configuration).process();
				new TopContentPagesBatch2(configuration).process();
				new TopPagesBatch2(configuration).process();
				new TopIdiomsBatch2(configuration).process();
				new AbsoluteValuesBatch2(configuration).process();
				
				// insert new record for time_status of processing
				session.execute("INSERT INTO batch_views.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", "servers_ranking", currentTime.getYear(), currentTime.getMonthValue(), currentTime.getDayOfMonth(), currentTime.getHour());
				
				this.setCurrentTime(getCurrentTime().plusHours(1));
				
			}
		}
		
	}
	
	public JavaRDD<TopResult> computeFullRankingFromPartial(String tableName) {
		
		return javaFunctions(this.sc)
			    .cassandraTable("batch_views", tableName, mapRowToTuple(String.class, Long.class))
			    .select("name", "count")
			    .mapToPair(row -> new Tuple2<String, Long>(row._1, row._2)).reduceByKey((a,b) -> a+b)
			    .map( tuple -> new TopResult(tuple._1, tuple._2, 
			    		getCurrentTime().getYear(), 
			    		getCurrentTime().getMonthValue(), 
			    		getCurrentTime().getDayOfMonth(), 
			    		getCurrentTime().getHour()));
		
//		javaFunctions(fullRanking).writerBuilder(servingKeyspace, tableName, rowWriterFactory);
//		
//		JavaRDD<Tuple2<String, Long>> partialRankings = javaFunctions(sc)
//				.cassandraTable("batch_views", tableName, mapRowTo(mapColumnTo(String.class), mapColumnTo(Long.class)))
//				.select("name", "count");
//		
//		CassandraJavaUtil.javaFunctions(sc).toJavaPairRDD(partialRankings, String.class, Long.class);
		
    }
		
	public abstract void process();
	
	public void finalizeSparkContext() {
		this.sc.close();
	}
	
	
}
