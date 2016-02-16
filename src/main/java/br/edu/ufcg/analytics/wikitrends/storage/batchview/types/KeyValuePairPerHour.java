package br.edu.ufcg.analytics.wikitrends.storage.batchview.types;

import java.io.Serializable;

public class KeyValuePairPerHour implements Serializable {
	
	private Integer hour;
	private Integer day;
	private Integer month;
	private Integer year;

	private String name;
	private Long value;
	public KeyValuePairPerHour(Integer year, Integer month, Integer day, Integer hour, String name, Long value) {
		super();
		this.year = year;
		this.month = month;
		this.day = day;
		this.hour = hour;
		this.name = name;
		this.value = value;
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
	 * @return the value
	 */
	public Long getValue() {
		return value;
	}
	/**
	 * @param value the value to set
	 */
	public void setValue(Long value) {
		this.value = value;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "KeyValuePairPerHour [hour=" + hour + ", day=" + day + ", month=" + month + ", year=" + year + ", name="
				+ name + ", value=" + value + "]";
	}
	

}
