package br.edu.ufcg.analytics.wikitrends.processing.batch1;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.util.Iterator;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;

import br.edu.ufcg.analytics.wikitrends.WikiTrendsCommands;
import br.edu.ufcg.analytics.wikitrends.WikiTrendsProcess;
import br.edu.ufcg.analytics.wikitrends.processing.AbstractBatchJob;
import br.edu.ufcg.analytics.wikitrends.processing.JobStatusID;
import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditChange;
import br.edu.ufcg.analytics.wikitrends.storage.serving1.types.TopClass;

/**
 * {@link WikiTrendsProcess} implementation when a {@link WikiTrendsCommands#BATCH} is chosen. 
 * 
 * @author Guilherme Gadelha
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 */
public abstract class BatchLayer1Job extends AbstractBatchJob {

	private static final long serialVersionUID = 833872580572610849L;

	private String batchViews1Keyspace;
	
	/**
	 * Default constructor.
	 * 
	 * NOTE: It takes the currentTime from status table on DB and
	 * add 1 hour to it. This currentTime is used to process the correct
	 * data on process() function.
	 * 
	 * @param configuration 
	 */
	public BatchLayer1Job(Configuration configuration, JobStatusID processStartTimeStatusId) {
		super(configuration, processStartTimeStatusId);
		setBatchViews1Keyspace(configuration.getString("wikitrends.serving1.cassandra.keyspace"));
	}
	
	public String getBatchViews1Keyspace() {
		return batchViews1Keyspace;
	}

	public void setBatchViews1Keyspace(String batchViewsKeyspace) {
		this.batchViews1Keyspace = batchViewsKeyspace;
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
	
	public void createJavaSparkContext(Configuration configuration) {
		SparkConf conf = new SparkConf();
		String appName = configuration.getString("wikitrends.job.batch1.id");
		Iterator<String> keys = configuration.getKeys();
		while (keys.hasNext()) {
			String key = keys.next();
			conf.set(key, configuration.getString(key));
		}
		
		if(configuration.containsKey("spark.master.host")) {
			String master_host = configuration.getString("spark.master.host");
			setJavaSparkContext(new JavaSparkContext(master_host, appName, conf));
		}
		else {
			setJavaSparkContext(new JavaSparkContext(conf.setAppName(appName)));
		}
	}
	
	public void run() {
		try (Cluster cluster = Cluster.builder().addContactPoints(getSeeds()).build();
				Session session = cluster.newSession();) {
			
			while(getCurrentTime().isBefore(getStopTime())) {
				process();
			
				session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
										getProcessStartTimeStatusID(), 
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
	
	public abstract void process();
	
}
