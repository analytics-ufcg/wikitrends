package br.edu.ufcg.analytics.wikitrends.datatypes;

import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

public class LogType extends AbstractType implements Serializable {
	private static final long serialVersionUID = -4354969409550959024L;

	private UUID log_uuid;
	private Integer log_id;
	private String log_type;
	private String log_action;
	private String log_params;
	private String log_action_comment;
	
	public LogType(String common_server_url, String common_server_name, String common_server_script_path,
			String common_server_wiki, String common_event_type, String common_event_namespace,
			String common_event_user, Boolean common_event_bot, String common_event_comment, String common_event_title,
			Date event_time, UUID log_uuid, Integer log_id, String log_type, String log_action, String log_params,
			String log_action_comment) {
		super(common_server_url, common_server_name, common_server_script_path, common_server_wiki, common_event_type,
				common_event_namespace, common_event_user, common_event_bot, common_event_comment, common_event_title,
				event_time);
		this.log_uuid = log_uuid;
		this.log_id = log_id;
		this.log_type = log_type;
		this.log_action = log_action;
		this.log_params = log_params;
		this.log_action_comment = log_action_comment;
	}

	public UUID getLog_uuid() {
		return log_uuid;
	}

	public void setLog_uuid(UUID log_uuid) {
		this.log_uuid = log_uuid;
	}

	public Integer getLog_id() {
		return log_id;
	}

	public void setLog_id(Integer log_id) {
		this.log_id = log_id;
	}

	public String getLog_type() {
		return log_type;
	}

	public void setLog_type(String log_type) {
		this.log_type = log_type;
	}

	public String getLog_action() {
		return log_action;
	}

	public void setLog_action(String log_action) {
		this.log_action = log_action;
	}

	public String getLog_params() {
		return log_params;
	}

	public void setLog_params(String log_params) {
		this.log_params = log_params;
	}

	public String getLog_action_comment() {
		return log_action_comment;
	}

	public void setLog_action_comment(String log_action_comment) {
		this.log_action_comment = log_action_comment;
	}

	
}
