package br.edu.ufcg.analytics.wikitrends.storage.raw.types;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.UUID;

import com.google.gson.JsonObject;

public class RawWikimediaChange implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3037162272247309636L;
	private UUID nonce;
	private Integer year;
	private Integer month;
	private Integer day;
	private Integer hour;
	private Date eventTimestamp;
	private String content;

	
	public RawWikimediaChange() {
		// TODO Auto-generated constructor stub
	}
	
	public RawWikimediaChange(UUID nonce, LocalDateTime eventTimestamp, String content) {
		
		this.nonce = nonce;
		this.eventTimestamp = Date.from(eventTimestamp.toInstant(ZoneOffset.UTC));
		setYear(eventTimestamp.getYear());
		setMonth(eventTimestamp.getMonthValue());
		setDay(eventTimestamp.getDayOfMonth());
		setHour(eventTimestamp.getHour());
		this.content = content;
	}

	/**
	 * @return the uuid
	 */
	public UUID getNonce() {
		return nonce;
	}

	/**
	 * @param nonce the uuid to set
	 */
	public void setNonce(UUID nonce) {
		this.nonce = nonce;
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
		return "RawWikimediaChange [nonce=" + nonce + ", year=" + year + ", month=" + month + ", day=" + day + ", hour="
				+ hour + ", eventTimestamp=" + eventTimestamp + ", content=" + content + "]";
	}
	
	public static RawWikimediaChange parseRawWikimediaChange(JsonObject object) {

		return new RawWikimediaChange(UUID.fromString(object.get("uuid").getAsString()),
				LocalDateTime.ofEpochSecond(object.get("timestamp").getAsLong(), 0, ZoneOffset.UTC), object.toString());
	}
	
	
	
}
