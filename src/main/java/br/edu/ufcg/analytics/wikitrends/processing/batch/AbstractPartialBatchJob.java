package br.edu.ufcg.analytics.wikitrends.processing.batch;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

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
public abstract class AbstractPartialBatchJob implements WikiTrendsProcess {

	private static final String statusTableName = BatchViewID.STATUS.toString();

	private static final long serialVersionUID = 833872580572610849L;

	private String keyspace;
	private transient JavaSparkContext sc;
	private BatchViewID batchViewID;
	

	/**
	 * Default constructor.
	 * 
	 * NOTE: It takes the currentTime from status table on DB and
	 * add 1 hour to it. This currentTime is used to process the correct
	 * data on process() function.
	 * 
	 * @param configuration 
	 */
	public AbstractPartialBatchJob(Configuration configuration, BatchViewID statusID) {
		this.sc = createJavaSparkContext(configuration);
		this.batchViewID = statusID;
		this.keyspace = configuration.getString("wikitrends.batchview.keyspace");
	}
	
	private JavaSparkContext createJavaSparkContext(Configuration configuration) {
		SparkConf conf = new SparkConf();
		conf.setAppName(this.getClass().getSimpleName());
		return new JavaSparkContext(conf);
	}
	
	public JavaSparkContext getJavaSparkContext() {
		return this.sc;
	}
	
	public String getBatchViewID() {
		return batchViewID.toString();
	}

	
	/**
	 * @return the keyspace
	 */
	public String getKeyspace() {
		return keyspace;
	}

	@Override
	public void run(String... args) {
		
		LocalDateTime currentTime = getStartTime(args);
		LocalDateTime stopTime = getFinishTime(args);
		
		LOGGER.info("Started job ".concat(this.getClass().getName()).concat(
				" with startTime= ").concat(currentTime.toString()).concat(" and stopTime= ").concat(stopTime.toString()));


		while(currentTime.isBefore(stopTime)) {
			process(currentTime);

			LOGGER.info("Job ".concat(this.getClass().getName()).concat(
					" processed with startTime= ").concat(currentTime.toString()).concat(" and stopTime= ").concat(stopTime.toString()));

			JavaRDD<JobStatus> rdd = getJavaSparkContext().parallelize(Arrays.asList(
					new JobStatus(
							getBatchViewID(), 
							currentTime.getYear(),
							currentTime.getMonthValue(),
							currentTime.getDayOfMonth(),
							currentTime.getHour())));

			CassandraJavaUtil.javaFunctions(rdd)
			.writerBuilder(getKeyspace(), statusTableName, mapToRow(JobStatus.class))
			.saveToCassandra();

			currentTime = currentTime.plusHours(1);
		}
	}

	private LocalDateTime getStartTime(String[] args) {
		if(args.length == 0){
			List<LocalDateTime> singleTimeCollection = javaFunctions(getJavaSparkContext())
					.cassandraTable(getKeyspace(), statusTableName).where("id = ?", getBatchViewID()).limit(1L)
					.map(row -> LocalDateTime
							.of(row.getInt("year"), row.getInt("month"), row.getInt("day"), row.getInt("hour"), 0)
							.plusHours(1))
					.collect();
			return singleTimeCollection.get(0);
		}else{
			return LocalDateTime.parse(args[0], DateTimeFormatter.ISO_LOCAL_DATE_TIME);
		}
	}

	private LocalDateTime getFinishTime(String[] args) {
		if(args.length == 2){
			return LocalDateTime.parse(args[1], DateTimeFormatter.ISO_LOCAL_DATE_TIME);
		}else{
			return LocalDateTime.ofInstant(Instant.ofEpochMilli((System.currentTimeMillis() / 3600000) * 3600000),
					ZoneId.systemDefault());
		}
	}
	
	protected abstract void process(LocalDateTime currentTime);

}
