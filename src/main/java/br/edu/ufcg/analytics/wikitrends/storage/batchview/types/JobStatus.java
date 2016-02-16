package br.edu.ufcg.analytics.wikitrends.storage.batchview.types;

import java.io.Serializable;

public class JobStatus implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6942577949280690639L;
	private String id;
	private Integer year;
	private Integer month;
	private Integer day;
	private Integer hour;
	
	public JobStatus() {
		// TODO Auto-generated constructor stub
	}
	
	public JobStatus(String id, Integer year, Integer month, Integer day, Integer hour) {
		super();
		this.id = id;
		this.year = year;
		this.month = month;
		this.day = day;
		this.hour = hour;
	}

	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @return the year
	 */
	public Integer getYear() {
		return year;
	}

	/**
	 * @param year the year to set
	 */
	public void setYear(Integer year) {
		this.year = year;
	}

	/**
	 * @return the month
	 */
	public Integer getMonth() {
		return month;
	}

	/**
	 * @param month the month to set
	 */
	public void setMonth(Integer month) {
		this.month = month;
	}

	/**
	 * @return the day
	 */
	public Integer getDay() {
		return day;
	}

	/**
	 * @param day the day to set
	 */
	public void setDay(Integer day) {
		this.day = day;
	}

	/**
	 * @return the hour
	 */
	public Integer getHour() {
		return hour;
	}

	/**
	 * @param hour the hour to set
	 */
	public void setHour(Integer hour) {
		this.hour = hour;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "JobStatus [id=" + id + ", year=" + year + ", month=" + month + ", day=" + day + ", hour=" + hour + "]";
	}
	
	
	
}
