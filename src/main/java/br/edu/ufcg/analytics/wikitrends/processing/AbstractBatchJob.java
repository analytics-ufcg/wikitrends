package br.edu.ufcg.analytics.wikitrends.processing;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.log4j.Logger;
//import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.edu.ufcg.analytics.wikitrends.WikiTrendsProcess;

public abstract class AbstractBatchJob implements WikiTrendsProcess {
	
	private static final long serialVersionUID = -6871759402025279789L;
	protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractBatchJob.class.getName());

	protected transient Configuration configuration;
	
	private LocalDateTime currentTime;
	private LocalDateTime stopTime;
	
	// transient: do not serialize this variable
	protected transient JavaSparkContext sc;

	private String PROCESS_STATUS_ID;
	
	public AbstractBatchJob(Configuration configuration, JobStatusID processStatusId) {
		this.configuration = configuration;
		
		createJavaSparkContext(this.configuration);
		
		setProcessStatusID(processStatusId);
		
		List<LocalDateTime> singleTimeCollection = javaFunctions(getJavaSparkContext()).cassandraTable("job_times", "status")
				.where("id = ?", getProcessStartTimeStatusID())
				.limit(1L)
				.map( row -> LocalDateTime.of(row.getInt("year"), row.getInt("month"), row.getInt("day"), row.getInt("hour"), 0).plusHours(1) )
				.collect();
		
		if(singleTimeCollection.isEmpty()){
			setCurrentTime(LocalDateTime.parse(configuration.getString("wikitrends.batch.incremental.starttime"), DateTimeFormatter.ISO_LOCAL_DATE_TIME));
			LOGGER.info("Getting startTime=".concat(getCurrentTime().toString()).concat(" from wikitrends.properties file"));
		}else{
			setCurrentTime(singleTimeCollection.get(0));
			LOGGER.info("Getting startTime=".concat(getCurrentTime().toString()).concat(" from job_times.status table"));
		}
		
		if(configuration.getBoolean("wikitrends.batch.incremental.stoptime.use", false)){
			setStopTime(LocalDateTime.parse(configuration.getString("wikitrends.batch.incremental.stoptime"), DateTimeFormatter.ISO_LOCAL_DATE_TIME));
			LOGGER.info("Getting stopTime=".concat(getCurrentTime().toString()).concat(" from wikitrends.properties file"));
		}else{
			setStopTime(LocalDateTime.ofInstant(Instant.ofEpochMilli((System.currentTimeMillis() / 3600000) * 3600000), ZoneId.systemDefault()));
			LOGGER.info("Getting stopTime=".concat(getCurrentTime().toString()).concat(" from OS"));
		}

		LOGGER.info("Started job ".concat(this.getClass().getName()).concat(
				" with startTime= ").concat(getCurrentTime().toString()).concat(" and stopTime= ").concat(getStopTime().toString()));
	}
	
	public String getProcessStartTimeStatusID() {
		return PROCESS_STATUS_ID;
	}

	public void setProcessStatusID(JobStatusID processStatusId) {
		this.PROCESS_STATUS_ID = processStatusId.getStatus_id();
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
	
	public abstract void createJavaSparkContext(Configuration configuration);
	
	public JavaSparkContext getJavaSparkContext() {
		return this.sc;
	}
	
	public void setJavaSparkContext(JavaSparkContext javaSparkContext) {
		this.sc = javaSparkContext;
	}
	
	public abstract void process();
	
	public abstract void run();

	public void finalizeSparkContext() {
		getJavaSparkContext().close();
	}
}
