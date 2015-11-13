package br.edu.ufcg.analytics.wikitrends.storage.serving.types;

import java.io.Serializable;
import java.util.Date;

public class AbsoluteValuesShot implements Serializable {
	private static final long serialVersionUID = -6188817936589241852L;

	private Integer id;
	private String date;
	private Integer all_edits;
	private Integer minor_edits;
	private Integer average_size;
	private Integer distinct_pages;
	private Integer distinct_editors;
	private Integer distinct_servers;
	private Long origin;
	private Long batch_elapsed_time;
	private Integer total_executor_cores;
	private Long input_size;
	private String hour;
	private Date event_time;

    public AbsoluteValuesShot() { }

    public AbsoluteValuesShot(Integer id, String date, String hour, Integer all_edits, Integer minor_edits,
    		Integer average_size, Integer distinct_pages, 
    		Integer distinct_editors, Integer distinct_servers,
    		Long origin, Long batch_elapsed_time,
    		Integer total_executor_cores, Long input_size,
    		Date event_time) {
        this.id = id;
    	this.date = date;
    	this.hour = hour;
    	this.all_edits = all_edits;
        this.minor_edits =	minor_edits;
        this.average_size = average_size;
        this.distinct_pages = distinct_pages;
        this.distinct_editors = distinct_editors;
        this.distinct_servers = distinct_servers;
        this.origin = origin; // in milliseconds (?)
        this.batch_elapsed_time = batch_elapsed_time; // in milliseconds (?)
        this.total_executor_cores = total_executor_cores;
        this.input_size = input_size; // in bytes (?)
        this.event_time = event_time;
    }
    
    public Integer getid() {
		return id;
	}

	public void setid(Integer id) {
		this.id = id;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public Integer getAll_edits() {
		return all_edits;
	}

	public void setAll_edits(Integer all_edits) {
		this.all_edits = all_edits;
	}

	public Integer getMinor_edits() {
		return minor_edits;
	}

	public void setMinor_edits(Integer minor_edits) {
		this.minor_edits = minor_edits;
	}

	public Integer getAverage_size() {
		return average_size;
	}

	public void setAverage_size(Integer average_size) {
		this.average_size = average_size;
	}

	public Integer getDistinct_pages() {
		return distinct_pages;
	}

	public void setDistinct_pages(Integer distinct_pages) {
		this.distinct_pages = distinct_pages;
	}

	public Integer getDistinct_editors() {
		return distinct_editors;
	}

	public void setDistinct_editors(Integer distinct_editors) {
		this.distinct_editors = distinct_editors;
	}

	public Integer getDistinct_servers() {
		return distinct_servers;
	}

	public void setDistinct_servers(Integer distinct_servers) {
		this.distinct_servers = distinct_servers;
	}

	public Long getOrigin() {
		return origin;
	}

	public void setOrigin(Long origin) {
		this.origin = origin;
	}

	public Long getBatch_elapsed_time() {
		return batch_elapsed_time;
	}

	public void setBatch_elapsed_time(Long batch_elapsed_time) {
		this.batch_elapsed_time = batch_elapsed_time;
	}

	public Integer getTotal_executor_cores() {
		return total_executor_cores;
	}

	public void setTotal_executor_cores(Integer total_executor_cores) {
		this.total_executor_cores = total_executor_cores;
	}

	public Long getInput_size() {
		return input_size;
	}

	public void setInput_size(Long input_size) {
		this.input_size = input_size;
	}

	public String getHour() {
		return hour;
	}

	public void setHour(String hour) {
		this.hour = hour;
	}

	public Date getEvent_time() {
		return event_time;
	}

	public void setEvent_time(Date event_time) {
		this.event_time = event_time;
	}

	@Override
	public String toString() {
		return "AbsoluteValuesShot [id=" + id + ", date=" + date + ", all_edits=" + all_edits + ", minor_edits="
				+ minor_edits + ", average_size=" + average_size + ", distinct_pages=" + distinct_pages
				+ ", distinct_editors=" + distinct_editors + ", distinct_servers=" + distinct_servers + ", origin="
				+ origin + ", batch_elapsed_time=" + batch_elapsed_time + ", total_executor_cores="
				+ total_executor_cores + ", input_size=" + input_size + ", hour=" + hour + ", event_time="
				+ event_time + "]";
	}
}
