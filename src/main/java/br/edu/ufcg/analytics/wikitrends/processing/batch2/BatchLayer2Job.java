package br.edu.ufcg.analytics.wikitrends.processing.batch2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowToTuple;

import java.util.Iterator;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import br.edu.ufcg.analytics.wikitrends.WikiTrendsCommands;
import br.edu.ufcg.analytics.wikitrends.WikiTrendsProcess;
import br.edu.ufcg.analytics.wikitrends.processing.AbstractBatchJob;
import br.edu.ufcg.analytics.wikitrends.processing.JobStatusID;
import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.TopResult;
import scala.Tuple2;

/**
 * {@link WikiTrendsProcess} implementation when a {@link WikiTrendsCommands#BATCH} is chosen. 
 * 
 * @author Guilherme Gadelha
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 */
public abstract class BatchLayer2Job extends AbstractBatchJob implements WikiTrendsProcess {

	private static final long serialVersionUID = 1218454132437246895L;
	
	private String batchViews2Keyspace;

	private String PROCESS_RESULT_ID;
	
	public BatchLayer2Job(Configuration configuration, JobStatusID processStatusId, ProcessResultID pId) {
		super(configuration, processStatusId);
		setBatchViews2Keyspace(configuration.getString("wikitrends.serving2.cassandra.keyspace"));
		setProcessResultID(pId);
	}
	
	
	public void setProcessResultID(ProcessResultID pId) {
		this.PROCESS_RESULT_ID = pId.getID();
	}
	
	public String getProcessResultID() {
		return this.PROCESS_RESULT_ID;
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
	
	/**
	 * Compute final ranking for the given table/process. 
	 * 
	 *  If final table is not empty it
	 * takes the values from the final table and merge them with the new values from the
	 * new hour.
	 * 
	 *  If the final table is empty it takes the values calculated from the current hour and
	 *  put them directly in the final table. 
	 * 
	 * @param tableName
	 * @return rdd of topResults
	 */
	public JavaRDD<TopResult> computeFullRankingFromPartial(String tableName) {
			return javaFunctions(getJavaSparkContext())
				    .cassandraTable("batch_views1", tableName, mapRowToTuple(String.class, Long.class))
				    .select("name", "count")
				    .mapToPair(row -> new Tuple2<String, Long>(row._1, row._2))
				    .reduceByKey((a,b) -> a+b)
				    .map( tuple -> new TopResult(getProcessResultID(), tuple._1, tuple._2));
		
    }

	protected void truncateResultingTable(String table) {
		try (Cluster cluster = Cluster.builder().addContactPoints(getSeeds()).build();
				Session session = cluster.newSession();) {
				session.execute("TRUNCATE TABLE " + getBatchViews2Keyspace() + "." + table);
		}
	}
	
	public void run(){
		logger.info("Started running job ".concat(this.getClass().getName()).concat(
				" since ").concat(getCurrentTime().toString()).concat(" using ").concat(
						configuration.getBoolean("wikitrends.batch.incremental.stoptime.use") == true ? 
								"property times" : "system times"));
		
		try (Cluster cluster = Cluster.builder().addContactPoints(getSeeds()).build();
				Session session = cluster.newSession();) {
			
			process();
			
			while(getCurrentTime().isBefore(getStopTime())) {
				session.execute("INSERT INTO job_times.status (id, year, month, day, hour) VALUES (?, ?, ?, ?, ?)", 
										getProcessStartTimeStatusID(), 
										getCurrentTime().getYear(), 
										getCurrentTime().getMonthValue(), 
										getCurrentTime().getDayOfMonth(), 
										getCurrentTime().getHour());
				
				this.setCurrentTime(getCurrentTime().plusHours(1));
			}
			
		} finally {
			logger.info("Ended running job ".concat(this.getClass().getName()).concat(
					" until ").concat(getStopTime().toString()).concat(" using ").concat(
							configuration.getBoolean("wikitrends.batch.incremental.stoptime.use") == true ? 
									"property times" : "system times"));
			
			finalizeSparkContext();
		}
	}
	
	public abstract void process();
}
