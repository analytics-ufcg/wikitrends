package br.edu.ufcg.analytics.wikitrends.processing;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import br.edu.ufcg.analytics.wikitrends.WikiTrendsProcess;

public abstract class AbstractBatchJob implements WikiTrendsProcess {
	
	private static final long serialVersionUID = -6871759402025279789L;

	protected transient Configuration configuration;
	
	private String[] seeds;
	
	private LocalDateTime currentTime;
	private LocalDateTime stopTime;
	
	// transient: do not serialize this variable
	private transient JavaSparkContext sc;

	private String PROCESS_STATUS_ID;
	
	public AbstractBatchJob(Configuration configuration, JobStatusID processStatusId) {
		createJavaSparkContext(configuration);
		
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
				setCurrentTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(configuration.getLong("wikitrends.batch.incremental.starttime") * 1000), ZoneId.systemDefault()));
			}
		}

		setStopTime(LocalDateTime.ofInstant(Instant.ofEpochMilli((System.currentTimeMillis() / 3600000) * 3600000), ZoneId.systemDefault()));
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
	
	public void finalizeSparkContext() {
		getJavaSparkContext().close();
	}
}
