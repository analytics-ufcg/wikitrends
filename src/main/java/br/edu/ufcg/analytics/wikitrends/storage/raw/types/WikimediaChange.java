package br.edu.ufcg.analytics.wikitrends.storage.raw.types;

import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

public abstract class WikimediaChange implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -451267642485797318L;
	
	private UUID nonce;
	private Integer id;
	private String serverUrl;
	private String serverName;
	private String serverScriptPath;
	private String wiki;
			
	private String type;
	private Integer namespace;
	private String user;
	private Boolean bot;
	private String comment;
	private String title;
			
	private Integer year;
	private Integer month;
	private Integer day;
	private Integer hour;
	private Date eventTimestamp;

	
	public WikimediaChange() {
		// TODO Auto-generated constructor stub
	}
	
	public WikimediaChange(UUID nonce, Integer id, String serverUrl, String serverName, String serverScriptPath, String wiki, String type,
			Integer namespace, String user, Boolean bot, String comment, String title, Integer year, Integer month,
			Integer day, Integer hour, Date eventTimestamp) {
		
		this.nonce = nonce;
		this.id = id;
		this.serverUrl = serverUrl;
		this.serverName = serverName;
		this.serverScriptPath = serverScriptPath;
		this.wiki = wiki;
		this.type = type;
		this.namespace = namespace;
		this.user = user;
		this.bot = bot;
		this.comment = comment;
		this.title = title;
		this.year = year;
		this.month = month;
		this.day = day;
		this.hour = hour;
		this.eventTimestamp = eventTimestamp;
	}
	

	public UUID getNonce() {
		return nonce;
	}

	public void setNonce(UUID nonce) {
		this.nonce = nonce;
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
	 * @return the serverUrl
	 */
	public String getServerUrl() {
		return serverUrl;
	}

	/**
	 * @param serverUrl the serverUrl to set
	 */
	public void setServerUrl(String serverUrl) {
		this.serverUrl = serverUrl;
	}

	/**
	 * @return the serverName
	 */
	public String getServerName() {
		return serverName;
	}

	/**
	 * @param serverName the serverName to set
	 */
	public void setServerName(String serverName) {
		this.serverName = serverName;
	}

	/**
	 * @return the serverScriptPath
	 */
	public String getServerScriptPath() {
		return serverScriptPath;
	}

	/**
	 * @param serverScriptPath the serverScriptPath to set
	 */
	public void setServerScriptPath(String serverScriptPath) {
		this.serverScriptPath = serverScriptPath;
	}

	/**
	 * @return the wiki
	 */
	public String getWiki() {
		return wiki;
	}

	/**
	 * @param wiki the wiki to set
	 */
	public void setWiki(String wiki) {
		this.wiki = wiki;
	}

	/**
	 * @return the type
	 */
	public String getType() {
		return type;
	}

	/**
	 * @param type the type to set
	 */
	public void setType(String type) {
		this.type = type;
	}

	/**
	 * @return the namespace
	 */
	public Integer getNamespace() {
		return namespace;
	}

	/**
	 * @param namespace the namespace to set
	 */
	public void setNamespace(Integer namespace) {
		this.namespace = namespace;
	}

	/**
	 * @return the user
	 */
	public String getUser() {
		return user;
	}

	/**
	 * @param user the user to set
	 */
	public void setUser(String user) {
		this.user = user;
	}

	/**
	 * @return the bot
	 */
	public Boolean getBot() {
		return bot;
	}

	/**
	 * @param bot the bot to set
	 */
	public void setBot(Boolean bot) {
		this.bot = bot;
	}

	/**
	 * @return the comment
	 */
	public String getComment() {
		return comment;
	}

	/**
	 * @param comment the comment to set
	 */
	public void setComment(String comment) {
		this.comment = comment;
	}

	/**
	 * @return the title
	 */
	public String getTitle() {
		return title;
	}

	/**
	 * @param title the title to set
	 */
	public void setTitle(String title) {
		this.title = title;
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

	
	@Override
	public String toString() {
		return "WikimediaChange [uuid=" + nonce + ", id=" + id + ", serverUrl=" + serverUrl + ", serverName="
				+ serverName + ", serverScriptPath=" + serverScriptPath + ", wiki=" + wiki + ", type=" + type
				+ ", namespace=" + namespace + ", user=" + user + ", bot=" + bot + ", comment=" + comment + ", title="
				+ title + ", year=" + year + ", month=" + month + ", day=" + day + ", hour=" + hour
				+ ", eventTimestamp=" + eventTimestamp + "]";
	}
	
}
