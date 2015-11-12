package br.edu.ufcg.analytics.wikitrends.datatypes;

import java.io.Serializable;
import java.util.Date;

import org.joda.time.DateTime;

public class AbstractType implements Serializable {
	private static final long serialVersionUID = -3986062084077684976L;

	private String common_server_url;
	private String common_server_name;
	private String common_server_script_path;
	private String common_server_wiki;
			
	private String common_event_type;
	private String common_event_namespace;
	private String common_event_user;
	private Boolean common_event_bot;
	private String common_event_comment;
	private String common_event_title;
			
	private Integer year;
	private Integer month;
	private Integer day;
	private Integer hour;
	private Date event_time;
	
	public AbstractType(String common_server_url, String common_server_name, String common_server_script_path,
			String common_server_wiki, String common_event_type, String common_event_namespace,
			String common_event_user, Boolean common_event_bot, String common_event_comment, String common_event_title,
			Date event_time) {
		super();
		this.common_server_url = common_server_url;
		this.common_server_name = common_server_name;
		this.common_server_script_path = common_server_script_path;
		this.common_server_wiki = common_server_wiki;
		this.common_event_type = common_event_type;
		this.common_event_namespace = common_event_namespace;
		this.common_event_user = common_event_user;
		this.common_event_bot = common_event_bot;
		this.common_event_comment = common_event_comment;
		this.common_event_title = common_event_title;
		this.event_time = event_time;
		
		DateTime date = new DateTime(this.event_time.getTime()); 
		setYear(date.getYear());
		setMonth(date.getMonthOfYear());
		setDay(date.getDayOfMonth());
		setHour(date.getHourOfDay());
	}

	public String getCommon_server_url() {
		return common_server_url;
	}

	public void setCommon_server_url(String common_server_url) {
		this.common_server_url = common_server_url;
	}

	public String getCommon_server_name() {
		return common_server_name;
	}

	public void setCommon_server_name(String common_server_name) {
		this.common_server_name = common_server_name;
	}

	public String getCommon_server_script_path() {
		return common_server_script_path;
	}

	public void setCommon_server_script_path(String common_server_script_path) {
		this.common_server_script_path = common_server_script_path;
	}

	public String getCommon_server_wiki() {
		return common_server_wiki;
	}

	public void setCommon_server_wiki(String common_server_wiki) {
		this.common_server_wiki = common_server_wiki;
	}

	public String getCommon_event_type() {
		return common_event_type;
	}

	public void setCommon_event_type(String common_event_type) {
		this.common_event_type = common_event_type;
	}

	public String getCommon_event_namespace() {
		return common_event_namespace;
	}

	public void setCommon_event_namespace(String common_event_namespace) {
		this.common_event_namespace = common_event_namespace;
	}

	public String getCommon_event_user() {
		return common_event_user;
	}

	public void setCommon_event_user(String common_event_user) {
		this.common_event_user = common_event_user;
	}

	public Boolean getCommon_event_bot() {
		return common_event_bot;
	}

	public void setCommon_event_bot(Boolean common_event_bot) {
		this.common_event_bot = common_event_bot;
	}

	public String getCommon_event_comment() {
		return common_event_comment;
	}

	public void setCommon_event_comment(String common_event_comment) {
		this.common_event_comment = common_event_comment;
	}

	public String getCommon_event_title() {
		return common_event_title;
	}

	public void setCommon_event_title(String common_event_title) {
		this.common_event_title = common_event_title;
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

	public Date getEvent_time() {
		return event_time;
	}

	public void setEvent_time(Date event_time) {
		this.event_time = event_time;
	}

	@Override
	public String toString() {
		return "AbstractType [common_server_url=" + common_server_url + ", common_server_name=" + common_server_name
				+ ", common_server_script_path=" + common_server_script_path + ", common_server_wiki="
				+ common_server_wiki + ", common_event_type=" + common_event_type + ", common_event_namespace="
				+ common_event_namespace + ", common_event_user=" + common_event_user + ", common_event_bot="
				+ common_event_bot + ", common_event_comment=" + common_event_comment + ", common_event_title="
				+ common_event_title + ", year=" + year + ", month=" + month + ", day=" + day + ", hour=" + hour
				+ ", event_time=" + event_time + "]";
	}
	
}
