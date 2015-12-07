package br.edu.ufcg.analytics.wikitrends.processing.batch1;

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

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

/**
 * {@link WikiTrendsProcess} implementation when a {@link WikiTrendsCommands#BATCH} is chosen. 
 * 
 * @author Guilherme Gadelha
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 */
public abstract class BatchLayer1Job implements WikiTrendsProcess {

	private static final long serialVersionUID = 833872580572610849L;

	protected transient Configuration configuration;
	
	private LocalDateTime currentTime;
	private LocalDateTime stopTime;
	private String[] seeds;

	private String batchViewsKeyspace;
	
	// transient: do not serialize this variable
	private transient JavaSparkContext sc;
	
	/**
	 * Default constructor
	 * @param configuration 
	 */
	public BatchLayer1Job(Configuration configuration, JavaSparkContext jsc) {
//		if(jsc == null) {
		createJavaSparkContext(configuration);
//		}
//		else {
//			setJavaSparkContext(jsc);
//		}
		
		setBatchViewsKeyspace(configuration.getString("wikitrends.batch.cassandra.keyspace"));
		
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

	public String getBatchViewsKeyspace() {
		return batchViewsKeyspace;
	}

	public void setBatchViewsKeyspace(String batchViewsKeyspace) {
		this.batchViewsKeyspace = batchViewsKeyspace;
	}
	
	public void createJavaSparkContext(Configuration configuration) {
		SparkConf conf = new SparkConf();
		String appName = configuration.getString("wikitrends.job.batch.id");
		String master_host = configuration.getString("spark.master.host");
		
		Iterator<String> keys = configuration.getKeys();
		while (keys.hasNext()) {
			String key = keys.next();
			conf.set(key, configuration.getString(key));
		}
		sc = new JavaSparkContext(master_host, appName, conf);
	}
	
//	public void setJavaSparkContext(JavaSparkContext javaSparkContext) {
//		this.sc = javaSparkContext;
//	}
	
	public JavaSparkContext getJavaSparkContext() {
		return this.sc;
	}
	
	@Override
	public void run() {

		try (Cluster cluster = Cluster.builder().addContactPoints(seeds).build();
				Session session = cluster.newSession();) {

			while(getCurrentTime().isBefore(getStopTime())){
				new TopEditorsBatch1(configuration, null).process();
				new TopContentPagesBatch1(configuration, null).process();
				new TopPagesBatch1(configuration, null).process();
				new TopIdiomsBatch1(configuration, null).process();
				new AbsoluteValuesBatch1(configuration, null).process();
				
				// insert new record for time_status of processing
				session.execute("INSERT INTO batch_views.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", "servers_ranking", currentTime.getYear(), currentTime.getMonthValue(), currentTime.getDayOfMonth(), currentTime.getHour());
				
				setCurrentTime(getCurrentTime().plusHours(1));
			}
		}

	}
	
	public JavaRDD<EditType> read() {
		JavaRDD<EditType> wikipediaEdits = javaFunctions(getJavaSparkContext()).cassandraTable("master_dataset", "edits")
				.select("event_time", "common_event_bot", "common_event_title", "common_server_name", "common_event_user",
						"common_event_namespace", "edit_minor", "edit_length")
				.where("year = ? and month = ? and day = ? and hour = ?", getCurrentTime().getYear(), getCurrentTime().getMonthValue(), getCurrentTime().getDayOfMonth(), getCurrentTime().getHour())
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
	
	protected JavaRDD<TopClass> transformToTopEntry(JavaPairRDD<String,Integer> pairRDD) {
		JavaRDD<TopClass> result = pairRDD
				.reduceByKey( (a,b) -> a+b )
				.map( edit -> new TopClass(edit._1, (long) edit._2, getCurrentTime().getYear(), getCurrentTime().getMonthValue(), getCurrentTime().getDayOfMonth(), getCurrentTime().getHour()) );
		
		return result;
	}
	
	public abstract void process();
	
	public void finalizeSparkContext() {
		this.sc.close();
	}
	
}
