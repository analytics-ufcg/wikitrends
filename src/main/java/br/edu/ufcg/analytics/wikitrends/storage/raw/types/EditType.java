package br.edu.ufcg.analytics.wikitrends.storage.raw.types;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.joda.time.DateTime;

import com.google.gson.JsonObject;

public class EditType extends AbstractType implements Serializable {
	private static final long serialVersionUID = 6352766661377046971L;
	
	private UUID edit_uuid; 
	private Integer edit_id;
	private Boolean editMinor;
	private Boolean edit_patrolled;
	private Map<String, Long> edit_length;
	private Map<String, Long> edit_revision;
	
	public EditType() {
		super();
	}

	public EditType(String common_server_url, String common_server_name, String common_server_script_path,
			String common_server_wiki, String common_event_type, Integer common_event_namespace,
			String common_event_user, Boolean common_event_bot, String common_event_comment, String common_event_title,
			Date event_time, UUID edit_uuid, Integer edit_id, Boolean edit_minor, Boolean edit_patrolled,
			Map<String, Long> edit_length, Map<String, Long> edit_revision) {
		super(common_server_url, common_server_name, common_server_script_path, common_server_wiki, common_event_type,
				common_event_namespace, common_event_user, common_event_bot, common_event_comment, common_event_title,
				event_time);
		this.edit_uuid = edit_uuid;
		this.edit_id = edit_id;
		this.editMinor = edit_minor;
		this.edit_patrolled = edit_patrolled;
		this.edit_length = edit_length;
		this.edit_revision = edit_revision;
	}

	public UUID getEdit_uuid() {
		return edit_uuid;
	}

	public void setEdit_uuid(UUID edit_uuid) {
		this.edit_uuid = edit_uuid;
	}

	public Integer getEdit_id() {
		return edit_id;
	}

	public void setEdit_id(Integer edit_id) {
		this.edit_id = edit_id;
	}

	public Boolean getEditMinor() {
		return editMinor;
	}

	public void setEditMinor(Boolean edit_minor) {
		this.editMinor = edit_minor;
	}

	public Boolean getEdit_patrolled() {
		return edit_patrolled;
	}

	public void setEdit_patrolled(Boolean edit_patrolled) {
		this.edit_patrolled = edit_patrolled;
	}

	public Map<String, Long> getEdit_length() {
		return edit_length;
	}

	public void setEdit_length(Map<String, Long> edit_length) {
		this.edit_length = edit_length;
	}

	public Map<String, Long> getEdit_revision() {
		return edit_revision;
	}

	public void setEdit_revision(Map<String, Long> edit_revision) {
		this.edit_revision = edit_revision;
	}

	@Override
	public String toString() {
		return "EditType [edit_uuid=" + edit_uuid + ", edit_id=" + edit_id + ", edit_minor=" + editMinor
				+ ", edit_patrolled=" + edit_patrolled + ", edit_length=" + edit_length + ", edit_revision="
				+ edit_revision + ", toString()=" + super.toString() + "]";
	}
	
	public static EditType parseEditFromJSON(JsonObject obj) {
		JsonObject length = obj.get("length").getAsJsonObject();

		HashMap<String, Long> lengthMap = new HashMap<>(2);
		if (!length.get("new").isJsonNull()) {
			lengthMap.put("new", length.get("new").getAsLong());
		}
		if (!length.get("old").isJsonNull()) {
			lengthMap.put("old", length.get("old").getAsLong());
		}

		JsonObject review = obj.get("revision").getAsJsonObject();

		HashMap<String, Long> revisionMap = new HashMap<>(2);
		if (!review.get("new").isJsonNull()) {
			revisionMap.put("new", review.get("new").getAsLong());
		}
		if (!review.get("old").isJsonNull()) {
			revisionMap.put("old", review.get("old").getAsLong());
		}

		Boolean patrolled = obj.has("patrolled") && !obj.get("patrolled").isJsonNull()
				? obj.get("patrolled").getAsBoolean() : null;

				return new EditType(obj.get("server_url").getAsString(), obj.get("server_name").getAsString(),
						obj.get("server_script_path").getAsString(), obj.get("wiki").getAsString(),
						obj.get("type").getAsString(), obj.get("namespace").getAsInt(), obj.get("user").getAsString(),
						obj.get("bot").getAsBoolean(), obj.get("comment").getAsString(), obj.get("title").getAsString(),
						new DateTime(obj.get("timestamp").getAsLong() * 1000L).toDate(), UUID.randomUUID(),
						obj.get("id").getAsInt(), obj.get("minor").getAsBoolean(), patrolled, lengthMap, revisionMap);
	}

}
