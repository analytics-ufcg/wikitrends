package br.edu.ufcg.analytics.wikitrends.processing.batch;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.WikiTrendsCommands;
import br.edu.ufcg.analytics.wikitrends.WikiTrendsProcess;
import br.edu.ufcg.analytics.wikitrends.storage.batchview.types.JobStatus;

/**
 * {@link WikiTrendsProcess} implementation when a {@link WikiTrendsCommands#BATCH} is chosen. 
 * 
 * @author Guilherme Gadelha
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 */
public abstract class AbstractFinalBatchJob implements WikiTrendsProcess {

	private static final long serialVersionUID = 1218454132437246895L;

	private String keyspace;
	private BatchViewID inputBatchViewID;
	private BatchViewID outputBatchViewID;
	private transient JavaSparkContext sc;
	
	
	public AbstractFinalBatchJob(Configuration configuration, BatchViewID inputBatchViewID, BatchViewID outputBatchViewID) {
		this.inputBatchViewID = inputBatchViewID;
		this.outputBatchViewID = outputBatchViewID;
		this.sc = createJavaSparkContext(configuration);
		this.keyspace = configuration.getString("wikitrends.batchview.keyspace");
	}

	private JavaSparkContext createJavaSparkContext(Configuration configuration) {
		SparkConf conf = new SparkConf();
		conf.setAppName(this.getClass().getSimpleName());
		return new JavaSparkContext(conf);
	}
	
	

	/**
	 * @return the sc
	 */
	public JavaSparkContext getJavaSparkContext() {
		return sc;
	}
	

	/**
	 * @return the rankingsTable
	 */
	public String getInputBatchViewID() {
		return inputBatchViewID.toString();
	}

	/**
	 * @return the statusID
	 */
	public String getOutputBatchViewID() {
		return outputBatchViewID.toString();
	}

	public String getKeyspace() {
		return keyspace;
	}


	public void run(String... args){

		process();

		LocalDateTime currentTime = LocalDateTime.ofInstant(Instant.ofEpochMilli((System.currentTimeMillis() / 3600000) * 3600000), ZoneId.systemDefault());

		LOGGER.info("Getting time=".concat(currentTime.toString()).concat(" from OS"));

		LOGGER.info("Job ".concat(this.getClass().getName()).concat(
				" processed with time= ").concat(currentTime.toString()));

		JavaRDD<JobStatus> rdd = getJavaSparkContext().parallelize(Arrays.asList(
				new JobStatus(
						getOutputBatchViewID(), 
						currentTime.getYear(),
						currentTime.getMonthValue(),
						currentTime.getDayOfMonth(),
						currentTime.getHour())));

		CassandraJavaUtil.javaFunctions(rdd)
		.writerBuilder(keyspace, BatchViewID.STATUS.toString(), mapToRow(JobStatus.class))
		.saveToCassandra();
	}

	public abstract void process();
}
