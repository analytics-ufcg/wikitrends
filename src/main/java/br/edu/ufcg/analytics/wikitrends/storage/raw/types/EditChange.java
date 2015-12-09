package br.edu.ufcg.analytics.wikitrends.storage.raw.types;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.joda.time.DateTime;

import com.google.gson.JsonObject;

public class EditChange extends WikimediaChange implements Serializable {
	
	private static final long serialVersionUID = 6352766661377046971L;

//	private UUID edit_uuid; 
	private Boolean minor;
	private Boolean patrolled;
	private Map<String, Long> length;
	private Map<String, Long> revision;
	
	public EditChange() {
		// TODO Auto-generated constructor stub
	}

	public EditChange(UUID uuid, Integer id, String serverUrl, String serverName, String serverScriptPath, String wiki, String type,
			Integer namespace, String user, Boolean bot, String comment, String title, Date eventTimestamp, Boolean minor, Boolean patrolled, Map<String, Long> length,
			Map<String, Long> revision) {
		super(uuid, id, serverUrl, serverName, serverScriptPath, wiki, type, namespace, user, bot, comment, title, eventTimestamp.getYear(), eventTimestamp.getMonth(),
				eventTimestamp.getDay(), eventTimestamp.getHours(), eventTimestamp);
		this.minor = minor;
		this.patrolled = patrolled;
		this.length = length;
		this.revision = revision;
	}



	/**
	 * @return the minor
	 */
	public Boolean getMinor() {
		return minor;
	}



	/**
	 * @param minor the minor to set
	 */
	public void setMinor(Boolean minor) {
		this.minor = minor;
	}



	/**
	 * @return the patrolled
	 */
	public Boolean getPatrolled() {
		return patrolled;
	}



	/**
	 * @param patrolled the patrolled to set
	 */
	public void setPatrolled(Boolean patrolled) {
		this.patrolled = patrolled;
	}



	/**
	 * @return the length
	 */
	public Map<String, Long> getLength() {
		return length;
	}



	/**
	 * @param length the length to set
	 */
	public void setLength(Map<String, Long> length) {
		this.length = length;
	}



	/**
	 * @return the revision
	 */
	public Map<String, Long> getRevision() {
		return revision;
	}



	/**
	 * @param revision the revision to set
	 */
	public void setRevision(Map<String, Long> revision) {
		this.revision = revision;
	}
	

	@Override
	public String toString() {
		return "EditChange [minor=" + minor + ", patrolled=" + patrolled + ", length=" + length + ", revision="
				+ revision + ", getUuid()=" + getNonce() + ", getId()=" + getId() + ", getServerUrl()=" + getServerUrl()
				+ ", getServerName()=" + getServerName() + ", getServerScriptPath()=" + getServerScriptPath()
				+ ", getWiki()=" + getWiki() + ", getType()=" + getType() + ", getNamespace()=" + getNamespace()
				+ ", getUser()=" + getUser() + ", getBot()=" + getBot() + ", getComment()=" + getComment()
				+ ", getTitle()=" + getTitle() + ", getYear()=" + getYear() + ", getMonth()=" + getMonth()
				+ ", getDay()=" + getDay() + ", getHour()=" + getHour() + ", getEventTimestamp()=" + getEventTimestamp()
				+ ", toString()=" + super.toString() + ", getClass()=" + getClass() + ", hashCode()=" + hashCode()
				+ "]";
	}

	public static EditChange parseEditChange(JsonObject object) {
		JsonObject length = object.get("length").getAsJsonObject();

		HashMap<String, Long> lengthMap = new HashMap<>(2);
		if (!length.get("new").isJsonNull()) {
			lengthMap.put("new", length.get("new").getAsLong());
		}
		if (!length.get("old").isJsonNull()) {
			lengthMap.put("old", length.get("old").getAsLong());
		}

		JsonObject review = object.get("revision").getAsJsonObject();

		HashMap<String, Long> revisionMap = new HashMap<>(2);
		if (!review.get("new").isJsonNull()) {
			revisionMap.put("new", review.get("new").getAsLong());
		}
		if (!review.get("old").isJsonNull()) {
			revisionMap.put("old", review.get("old").getAsLong());
		}

		Boolean patrolled = object.has("patrolled") && !object.get("patrolled").isJsonNull()
				? object.get("patrolled").getAsBoolean() : null;

		UUID uuidFromBytes = object.has("uuid")? UUID.fromString(object.get("uuid").getAsString()): UUID.randomUUID();

		return new EditChange(uuidFromBytes, object.get("id").getAsInt(), object.get("server_url").getAsString(), object.get("server_name").getAsString(),
				object.get("server_script_path").getAsString(), object.get("wiki").getAsString(),
				object.get("type").getAsString(), object.get("namespace").getAsInt(), object.get("user").getAsString(),
				object.get("bot").getAsBoolean(), object.get("comment").getAsString(),
				object.get("title").getAsString(), new DateTime(object.get("timestamp").getAsLong() * 1000L).toDate(),
				object.get("minor").getAsBoolean(), patrolled,
				lengthMap, revisionMap);
	}

}