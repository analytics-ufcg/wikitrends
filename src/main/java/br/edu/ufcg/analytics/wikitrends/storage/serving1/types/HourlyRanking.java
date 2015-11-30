package br.edu.ufcg.analytics.wikitrends.storage.serving1.types;

import java.io.Serializable;

/**
 * Abstraction to 
 * @author Ricardo Araújo Santos - ricoaraujosantos@gmail.com
 */
public class HourlyRanking implements Serializable {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2622221765473245426L;
	
	private Integer year;
	private Integer month;
	private Integer hour;
	private Integer day;
	private String name;
	private Long count;
	
	public HourlyRanking() {
		
	}
	
	public HourlyRanking(int year, int month, int hour, int day, String keyName, long count) {
		this();
		this.year = year;
		this.month = month;
		this.hour = hour;
		this.day = day;
		this.name = keyName;
		this.count = count;
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
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the count
	 */
	public Long getCount() {
		return count;
	}

	/**
	 * @param count the count to set
	 */
	public void setCount(Long count) {
		this.count = count;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "HourlyRanking [year=" + year + ", month=" + month + ", hour=" + hour + ", day=" + day + ", keyName="
				+ name + ", count=" + count + "]";
	}
	
}