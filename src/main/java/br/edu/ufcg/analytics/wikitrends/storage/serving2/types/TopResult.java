package br.edu.ufcg.analytics.wikitrends.storage.serving2.types;

import java.io.Serializable;
import java.util.UUID;

public class TopResult implements Serializable {
	private static final long serialVersionUID = -4651390156724384971L;
	
	private UUID id;
	private String name;
	private Long count;

	private Integer year;
	private Integer month;
	private Integer day;
	private Integer hour;

	public TopResult(String name, Long count, Integer year, Integer month, Integer day, Integer hour) {
		this.id = UUID.randomUUID();
	    this.name = name;
	    this.count = count;
	    this.year = year;
	    this.month = month;
	    this.day = day;
	    this.hour = hour;
	}
	
	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

	public Integer getYear() {
		return year;
	}

	public void setYear(Integer year) {
		this.year = year;
	}

	public Integer getMonth() {
		return month;
	}

	public void setMonth(Integer month) {
		this.month = month;
	}

	public Integer getDay() {
		return day;
	}

	public void setDay(Integer day) {
		this.day = day;
	}

	public Integer getHour() {
		return hour;
	}

	public void setHour(Integer hour) {
		this.hour = hour;
	}

	@Override
	public String toString() {
		return "TopResult [id=" + id + ", name=" + name + ", count=" + count + ", year=" + year + ", month=" + month
				+ ", day=" + day + ", hour=" + hour + "]";
	}

}
