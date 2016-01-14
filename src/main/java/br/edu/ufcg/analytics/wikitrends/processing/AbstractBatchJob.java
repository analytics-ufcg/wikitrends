package br.edu.ufcg.analytics.wikitrends.processing;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import br.edu.ufcg.analytics.wikitrends.WikiTrendsProcess;

public abstract class AbstractBatchJob implements WikiTrendsProcess {
	
	private static final long serialVersionUID = -6871759402025279789L;
	private static final Logger logger = Logger.getLogger(AbstractBatchJob.class.getName());

	private transient Configuration configuration;
	
	private String[] seeds;
	
	private LocalDateTime currentTime;
	private LocalDateTime stopTime;
	
	// transient: do not serialize this variable
	private transient JavaSparkContext sc;

	private String PROCESS_STATUS_ID;
	
	public AbstractBatchJob(Configuration configuration, JobStatusID processStatusId) {
		this.configuration = configuration;
		
		PropertyConfigurator.configure("log4j.properties");
		
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
			} else {
				setCurrentTime(LocalDateTime.of(configuration.getInt("wikitrends.batch.incremental.starttime.year"),
											 configuration.getInt("wikitrends.batch.incremental.starttime.month"),
											 configuration.getInt("wikitrends.batch.incremental.starttime.day"),
											 configuration.getInt("wikitrends.batch.incremental.starttime.hour"), 0));
			}
		}
		
		if(configuration.getBoolean("wikitrends.batch.incremental.stoptime.use") == true) {
			setStopTime(LocalDateTime.of(configuration.getInt("wikitrends.batch.incremental.stoptime.year"),
										 configuration.getInt("wikitrends.batch.incremental.stoptime.month"),
										 configuration.getInt("wikitrends.batch.incremental.stoptime.day"),
										 configuration.getInt("wikitrends.batch.incremental.stoptime.hour"), 0));
		} else {
			setStopTime(LocalDateTime.ofInstant(Instant.ofEpochMilli((System.currentTimeMillis() / 3600000) * 3600000), ZoneId.systemDefault()));
		}
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
	
	public void run() {		
		logger.info("Started running job ".concat(this.getClass().getName()).concat(
				" since ").concat(getCurrentTime().toString()).concat(" using ").concat(
						configuration.getBoolean("wikitrends.batch.incremental.stoptime.use") == true ? 
								"property times" : "system times"));
		
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
			logger.info("Ended running job ".concat(this.getClass().getName()).concat(
					" until ").concat(getStopTime().toString()).concat(" using ").concat(
							configuration.getBoolean("wikitrends.batch.incremental.stoptime.use") == true ? 
									"property times" : "system times"));
			
			finalizeSparkContext();
		}
	}
	
	public void finalizeSparkContext() {
		getJavaSparkContext().close();
	}
}
