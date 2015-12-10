package br.edu.ufcg.analytics.wikitrends.storage.raw.types;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import com.google.gson.JsonObject;

public class LogChange extends WikimediaChange implements Serializable {
	

	private static final long serialVersionUID = -2770336521820400965L;
	
	private Integer logId;
	private String logType;
	private String logAction;
	private String logParams;
	private String logActionComment;
	
	public LogChange() {
		// TODO Auto-generated constructor stub
	}
	
	
	
	public LogChange(UUID nonce, Integer id, String serverUrl, String serverName, String serverScriptPath, String wiki, String type,
			Integer namespace, String user, Boolean bot, String comment, String title, LocalDateTime eventTimestamp, Integer logId, 
			String logType, String logAction, String logParams, String logActionComment) {
		
		super(nonce, id, serverUrl, serverName, serverScriptPath, wiki, type, namespace, user, bot, comment, title, eventTimestamp);
		this.logId = logId;
		this.logType = logType;
		this.logAction = logAction;
		this.logParams = logParams;
		this.logActionComment = logActionComment;
	}
	
	
	
	
	/**
	 * @return the logId
	 */
	public Integer getLogId() {
		return logId;
	}



	/**
	 * @param logId the logId to set
	 */
	public void setLogId(Integer logId) {
		this.logId = logId;
	}



	/**
	 * @return the logType
	 */
	public String getLogType() {
		return logType;
	}



	/**
	 * @param logType the logType to set
	 */
	public void setLogType(String logType) {
		this.logType = logType;
	}



	/**
	 * @return the logAction
	 */
	public String getLogAction() {
		return logAction;
	}



	/**
	 * @param logAction the logAction to set
	 */
	public void setLogAction(String logAction) {
		this.logAction = logAction;
	}



	/**
	 * @return the logParams
	 */
	public String getLogParams() {
		return logParams;
	}



	/**
	 * @param logParams the logParams to set
	 */
	public void setLogParams(String logParams) {
		this.logParams = logParams;
	}



	/**
	 * @return the logActionComment
	 */
	public String getLogActionComment() {
		return logActionComment;
	}



	/**
	 * @param logActionComment the logActionComment to set
	 */
	public void setLogActionComment(String logActionComment) {
		this.logActionComment = logActionComment;
	}
	


	@Override
	public String toString() {
		return "LogChange [logId=" + logId + ", logType=" + logType + ", logAction=" + logAction + ", logParams="
				+ logParams + ", logActionComment=" + logActionComment + ", getUuid()=" + getNonce() + ", getId()="
				+ getId() + ", getServerUrl()=" + getServerUrl() + ", getServerName()=" + getServerName()
				+ ", getServerScriptPath()=" + getServerScriptPath() + ", getWiki()=" + getWiki() + ", getType()="
				+ getType() + ", getNamespace()=" + getNamespace() + ", getUser()=" + getUser() + ", getBot()="
				+ getBot() + ", getComment()=" + getComment() + ", getTitle()=" + getTitle() + ", getYear()="
				+ getYear() + ", getMonth()=" + getMonth() + ", getDay()=" + getDay() + ", getHour()=" + getHour()
				+ ", getEventTimestamp()=" + getEventTimestamp() + ", toString()=" + super.toString() + ", getClass()="
				+ getClass() + ", hashCode()=" + hashCode() + "]";
	}



	public static LogChange parseLogChange(JsonObject object){
		String log_params = object.has("log_params") ? object.get("log_params").toString() : null;

		Integer id = object.has("id") && !object.get("id").isJsonNull() ? object.get("id").getAsInt() : null;

		String log_type = object.has("log_type") ? object.get("log_type").getAsString() : null;
		

		return new LogChange(UUID.fromString(object.get("uuid").getAsString()), id , object.get("server_url").getAsString(), object.get("server_name").getAsString(),
				object.get("server_script_path").getAsString(), object.get("wiki").getAsString(),
				object.get("type").getAsString(), object.get("namespace").getAsInt(), object.get("user").getAsString(),
				object.get("bot").getAsBoolean(), object.get("comment").getAsString(), object.get("title").getAsString(),
				LocalDateTime.ofEpochSecond(object.get("timestamp").getAsLong(), 0, ZoneOffset.UTC),
				object.get("log_id").getAsInt(), log_type, object.get("log_action").getAsString(), log_params,
				object.get("log_action_comment").getAsString());
	}

	
}
