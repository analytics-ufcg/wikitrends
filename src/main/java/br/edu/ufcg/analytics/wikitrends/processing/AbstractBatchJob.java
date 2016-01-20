package br.edu.ufcg.analytics.wikitrends.processing;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.log4j.Logger;
//import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import br.edu.ufcg.analytics.wikitrends.WikiTrendsProcess;

public abstract class AbstractBatchJob implements WikiTrendsProcess {
	
	private static final long serialVersionUID = -6871759402025279789L;
	protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractBatchJob.class.getName());

	protected transient Configuration configuration;
	
	private String[] seeds;
	
	private LocalDateTime currentTime;
	private LocalDateTime stopTime;
	
	// transient: do not serialize this variable
	protected transient JavaSparkContext sc;

	private String PROCESS_STATUS_ID;
	
	public AbstractBatchJob(Configuration configuration, JobStatusID processStatusId) {
		this.configuration = configuration;
		
//		PropertyConfigurator.configure(configuration.getString("wikitrends.log4j_properties_file_path"));
		
		createJavaSparkContext(this.configuration);
		
		setProcessStatusID(processStatusId);
		
		setSeeds(configuration.getStringArray("spark.cassandra.connection.host"));

		try (Cluster cluster = Cluster.builder().addContactPoints(getSeeds()).build();
				Session session = cluster.newSession();) {
			ResultSet resultSet = session.execute("SELECT * FROM job_times.status WHERE id = ? LIMIT 1", getProcessStartTimeStatusID());
			List<Row> all = resultSet.all();
			if(!all.isEmpty()){
				Row row = all.get(0);
				setCurrentTime(LocalDateTime.of(row.getInt("year"), row.getInt("month"), row.getInt("day"), row.getInt("hour"), 0).plusHours(1));
				
				LOGGER.info("Getting startTime=".concat(getCurrentTime().toString()).concat(" from job_times.status table"));
				
			} else {
				if(configuration.getBoolean("wikitrends.batch.incremental.starttime.use") == true) {
					setCurrentTime(LocalDateTime.of(configuration.getInt("wikitrends.batch.incremental.starttime.year"),
												 configuration.getInt("wikitrends.batch.incremental.starttime.month"),
												 configuration.getInt("wikitrends.batch.incremental.starttime.day"),
												 configuration.getInt("wikitrends.batch.incremental.starttime.hour"), 0));
					LOGGER.info("Getting startTime=".concat(getCurrentTime().toString()).concat(" from wikitrends.properties file"));
				}
				else {
					LOGGER.warn("Job starting with no starttime defined!");
				}
			}
		}
		
		if(configuration.getBoolean("wikitrends.batch.incremental.stoptime.use") == true) {
			setStopTime(LocalDateTime.of(configuration.getInt("wikitrends.batch.incremental.stoptime.year"),
										 configuration.getInt("wikitrends.batch.incremental.stoptime.month"),
										 configuration.getInt("wikitrends.batch.incremental.stoptime.day"),
										 configuration.getInt("wikitrends.batch.incremental.stoptime.hour"), 0));
			
			LOGGER.info("Getting stopTime=".concat(getStopTime().toString()).concat(" from wikitrends.properties table"));
			
		} else {
			setStopTime(LocalDateTime.ofInstant(Instant.ofEpochMilli((System.currentTimeMillis() / 3600000) * 3600000), ZoneId.systemDefault()));
			
			LOGGER.info("Getting stopTime=".concat(getStopTime().toString()).concat(" from OS"));
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
