package br.edu.ufcg.analytics.wikitrends.processing.batch2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowToTuple;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.Arrays;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.WikiTrendsCommands;
import br.edu.ufcg.analytics.wikitrends.WikiTrendsProcess;
import br.edu.ufcg.analytics.wikitrends.processing.AbstractBatchJob;
import br.edu.ufcg.analytics.wikitrends.processing.JobStatusID;
import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.RankingEntry;
import br.edu.ufcg.analytics.wikitrends.storage.status.JobStatus;
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
		conf.setAppName(this.getClass().getSimpleName());
		setJavaSparkContext(new JavaSparkContext(conf));
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
	public JavaRDD<RankingEntry> computeFullRankingFromPartial(String tableName) {
		return javaFunctions(getJavaSparkContext())
				.cassandraTable("batch_views1", tableName, mapRowToTuple(String.class, Long.class))
				.select("name", "count")
				.mapToPair(row -> new Tuple2<String, Long>(row._1, row._2))
				.reduceByKey((a,b) -> a+b)
				.mapToPair( pair -> pair.swap() )
				.sortByKey(false)
				.zipWithIndex()
				.map( tuple -> new RankingEntry(getProcessResultID(), tuple._2, tuple._1._2, tuple._1._1));
	}

	public void run(){

		process();

		LOGGER.info("Job ".concat(this.getClass().getName()).concat(
				" processed with startTime= ").concat(getCurrentTime().toString()).concat(" and stopTime= ").concat(getStopTime().toString()));

		while(getCurrentTime().isBefore(getStopTime())) {

			JavaRDD<JobStatus> rdd = getJavaSparkContext().parallelize(Arrays.asList(
					new JobStatus(
							getProcessStartTimeStatusID(), 
							getCurrentTime().getYear(),
							getCurrentTime().getMonthValue(),
							getCurrentTime().getDayOfMonth(),
							getCurrentTime().getHour())));

			CassandraJavaUtil.javaFunctions(rdd)
			.writerBuilder("job_times", "status", mapToRow(JobStatus.class))
			.saveToCassandra();


			this.setCurrentTime(getCurrentTime().plusHours(1));
		}
	}
	
	public abstract void process();
}
