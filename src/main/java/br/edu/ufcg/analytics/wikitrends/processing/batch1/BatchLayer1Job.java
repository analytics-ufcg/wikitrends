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
import br.edu.ufcg.analytics.wikitrends.processing.JobStatusID;
import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditChange;
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

	private String PROCESS_STATUS_ID;

	/**
	 * Default constructor.
	 * 
	 * NOTE: It takes the currentTime from status table on DB and
	 * add 1 hour to it. This currentTime is used to process the correct
	 * data on process() function.
	 * 
	 * @param configuration 
	 */
	public BatchLayer1Job(Configuration configuration, JobStatusID processStatusId) {
		createJavaSparkContext(configuration);
		
		setProcessStatusID(processStatusId);
		
		setBatchViewsKeyspace(configuration.getString("wikitrends.serving1.cassandra.keyspace"));
		
		setSeeds(configuration.getStringArray("spark.cassandra.connection.host"));

		try (Cluster cluster = Cluster.builder().addContactPoints(seeds).build();
				Session session = cluster.newSession();) {
			ResultSet resultSet = session.execute("SELECT * FROM job_times.status WHERE id = ? LIMIT 1", getProcessStatusID());
			List<Row> all = resultSet.all();
			if(!all.isEmpty()){
				Row row = all.get(0);
				setCurrentTime(LocalDateTime.of(row.getInt("year"), row.getInt("month"), row.getInt("day"), row.getInt("hour"), 0).plusHours(1));
			}else{
				setCurrentTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(configuration.getLong("wikitrends.batch.incremental.starttime") * 1000), ZoneId.systemDefault()));
			}
		}

		//	end = LocalDateTime.ofInstant(Instant.ofEpochMilli((System.currentTimeMillis() / 3600000) * 3600000), ZoneId.systemDefault());
		//  setStopTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(configuration.getLong("wikitrends.batch.incremental.stoptime") * 1000), ZoneId.systemDefault()));
		//	end = LocalDateTime.of(2015, 11, 9, 12, 0) ;
	}
	
	public String getProcessStatusID() {
		return PROCESS_STATUS_ID;
	}

	public void setProcessStatusID(JobStatusID topIdiomsStatusId) {
		this.PROCESS_STATUS_ID = topIdiomsStatusId.getStatus_id();
	}

	public String[] getSeeds() {
		return seeds;
	}

	public void setSeeds(String[] seeds) {
		this.seeds = seeds;
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
	
	public void setCurrentTime(LocalDateTime localDateTime) {
		this.currentTime = localDateTime;
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
	
	public JavaSparkContext getJavaSparkContext() {
		return this.sc;
	}
	
	@Override
	@Deprecated
	public void run() {

		try (Cluster cluster = Cluster.builder().addContactPoints(seeds).build();
				Session session = cluster.newSession();) {

			while(getCurrentTime().isBefore(getStopTime())){
				new TopEditorsBatch1(configuration).process();
				new TopContentPagesBatch1(configuration).process();
				new TopPagesBatch1(configuration).process();
				new TopIdiomsBatch1(configuration).process();
				new AbsoluteValuesBatch1(configuration).process();
				
				// insert new record for time_status of processing
				session.execute("INSERT INTO batch_views.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", "servers_ranking", currentTime.getYear(), currentTime.getMonthValue(), currentTime.getDayOfMonth(), currentTime.getHour());
				
				this.setCurrentTime(getCurrentTime().plusHours(1));
			}
		}

	}
	
	public JavaRDD<EditChange> read() {
		JavaRDD<EditChange> wikipediaEdits = javaFunctions(getJavaSparkContext()).cassandraTable("master_dataset", "edits")
				.select("event_timestamp", "bot", "title", "server_name", "user", "namespace", "minor", "length")
				.where("year = ? and month = ? and day = ? and hour = ?", getCurrentTime().getYear(), getCurrentTime().getMonthValue(), getCurrentTime().getDayOfMonth(), getCurrentTime().getHour())
				.map(new Function<CassandraRow, EditChange>() {
					private static final long serialVersionUID = 1L;

					@Override
					public EditChange call(CassandraRow v1) throws Exception {
						EditChange edit = new EditChange();
						edit.setEventTimestamp(v1.getDate("event_timestamp"));
						edit.setBot(v1.getBoolean("bot"));
						edit.setTitle(v1.getString("title"));
						edit.setUser(v1.getString("user"));
						edit.setNamespace(v1.getInt("namespace"));
						edit.setServerName(v1.getString("server_name"));
						edit.setMinor(v1.getBoolean("minor"));
						edit.setLength(v1.getMap("length", CassandraJavaUtil.typeConverter(String.class), CassandraJavaUtil.typeConverter(Long.class)));
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
	
	public void run2() {
		try (Cluster cluster = Cluster.builder().addContactPoints(getSeeds()).build();
				Session session = cluster.newSession();) {
			
			while(getCurrentTime().isBefore(getStopTime())) {
				process();
			
				session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
										PROCESS_STATUS_ID, 
										getCurrentTime().getYear(), 
										getCurrentTime().getMonthValue(), 
										getCurrentTime().getDayOfMonth(), 
										getCurrentTime().getHour());
				
				this.setCurrentTime(getCurrentTime().plusHours(1));
			}
		} finally {
			finalizeSparkContext();
		}
	}
	
	public void finalizeSparkContext() {
		this.sc.close();
	}
	
}
