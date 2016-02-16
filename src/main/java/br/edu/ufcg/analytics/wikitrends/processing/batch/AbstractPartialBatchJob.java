package br.edu.ufcg.analytics.wikitrends.processing.batch1;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.Arrays;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.WikiTrendsCommands;
import br.edu.ufcg.analytics.wikitrends.WikiTrendsProcess;
import br.edu.ufcg.analytics.wikitrends.processing.AbstractBatchJob;
import br.edu.ufcg.analytics.wikitrends.processing.JobStatusID;
import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditChange;
import br.edu.ufcg.analytics.wikitrends.storage.serving1.types.TopClass;
import br.edu.ufcg.analytics.wikitrends.storage.status.JobStatus;

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
	
	public abstract JavaRDD<EditChange> read();

	protected JavaRDD<TopClass> transformToTopEntry(JavaPairRDD<String,Integer> pairRDD) {
		JavaRDD<TopClass> result = pairRDD
				.reduceByKey( (a,b) -> a+b )
				.map( edit -> new TopClass(edit._1, (long) edit._2, getCurrentTime().getYear(), getCurrentTime().getMonthValue(), getCurrentTime().getDayOfMonth(), getCurrentTime().getHour()) );
		
		return result;
	}
	
	public void createJavaSparkContext(Configuration configuration) {
		SparkConf conf = new SparkConf();
		conf.setAppName(this.getClass().getSimpleName());
		setJavaSparkContext(new JavaSparkContext(conf));
	}
	
	public void run() {

		while(getCurrentTime().isBefore(getStopTime())) {
			process();

			LOGGER.info("Job ".concat(this.getClass().getName()).concat(
					" processed with startTime= ").concat(getCurrentTime().toString()).concat(" and stopTime= ").concat(getStopTime().toString()));

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
