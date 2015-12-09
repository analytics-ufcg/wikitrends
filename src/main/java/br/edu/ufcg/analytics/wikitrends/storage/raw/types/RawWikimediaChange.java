package br.edu.ufcg.analytics.wikitrends.storage.raw.types;

import java.io.Serializable;
import java.util.Date;

import org.joda.time.DateTime;

import com.google.gson.JsonObject;

public class RawWikimediaChange implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3037162272247309636L;
	private Integer id;
	private Integer year;
	private Integer month;
	private Integer day;
	private Integer hour;
	private Date eventTimestamp;
	private String content;

	
	public RawWikimediaChange() {
		// TODO Auto-generated constructor stub
	}
	
	public RawWikimediaChange(Integer id, Date eventTimestamp, String content) {
		
		this.id = id;
		this.year = eventTimestamp.getYear();
		this.month = eventTimestamp.getMonth();
		this.day = eventTimestamp.getDay();
		this.hour = eventTimestamp.getHours();
		this.eventTimestamp = eventTimestamp;
		this.content = content;
	}

	/**
	 * @return the id
	 */
	public Integer getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(Integer id) {
		this.id = id;
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

	/**
	 * @return the eventTimestamp
	 */
	public Date getEventTimestamp() {
		return eventTimestamp;
	}

	/**
	 * @param eventTimestamp the eventTimestamp to set
	 */
	public void setEventTimestamp(Date eventTimestamp) {
		this.eventTimestamp = eventTimestamp;
	}

	/**
	 * @return the content
	 */
	public String getContent() {
		return content;
	}

	/**
	 * @param content the content to set
	 */
	public void setContent(String content) {
		this.content = content;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "RawWikimediaChange [id=" + id + ", year=" + year + ", month=" + month + ", day=" + day + ", hour="
				+ hour + ", eventTimestamp=" + eventTimestamp + ", content=" + content + "]";
	}
	
	public static RawWikimediaChange parseRawWikimediaChange(JsonObject object){

		return new RawWikimediaChange(object.get("id").getAsInt(), new DateTime(object.get("timestamp").getAsLong() * 1000L).toDate(),
				object.toString());
	}

	
	
	
}
