package br.edu.ufcg.analytics.wikitrends.storage.raw;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.io.Serializable;
import java.util.HashMap;
import java.util.UUID;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditType;
import br.edu.ufcg.analytics.wikitrends.storage.raw.types.LogType;

public class CassandraMasterDatasetManager implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6066687152165846375L;

	/**
	 * @param session Opened {@link Session} to a cassandra DB.
	 */
	public void createTables(Session session) {

		session.execute("DROP KEYSPACE IF EXISTS master_dataset");

		session.execute("CREATE KEYSPACE master_dataset WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

		session.execute("CREATE TABLE IF NOT EXISTS master_dataset.logs(" +
				"log_uuid UUID," +
				"id INT," +
				"log_id INT," +
				"log_action TEXT," +
				"log_type TEXT," +
				"log_params TEXT," +
				"log_action_comment TEXT," +

	            "common_server_url TEXT," +
	            "common_server_name TEXT," +
	            "common_server_script_path TEXT," +
	            "common_server_wiki TEXT," +

				"common_event_type TEXT," +
				"common_event_namespace INT," +
				"common_event_user TEXT," +
				"common_event_bot BOOLEAN," +
				"common_event_comment TEXT," +
				"common_event_title TEXT," +

				"year INT," +
				"month INT," +
				"day INT," +
				"hour INT," +
				"event_time TIMESTAMP," +

				"PRIMARY KEY((log_uuid), year, month, day, hour)," +
				") WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC);"
				);

		// to types 'edit' and 'external'
//		session.execute("CREATE TABLE IF NOT EXISTS master_dataset.edits("+ 
//				//				"edit_uuid UUID," + 
//				"id uuid," +
//				"minor boolean," +
//				"patrolled boolean," +
//				"length map<text, int>," +
//				"revision map<text, int>," +
//
//	            "server_url text," +
//	            "server_name text," +
//	            "server_script_path text," +
//	            "server_wiki text," +
//
//				"event_type text," +
//				"event_namespace int," +
//				"event_user text," +
//				"event_bot boolean," +
//				"event_comment text," +
//				"event_title text," +
//
//				"year int," +
//				"month int," +
//				"day int," +
//				"hour int," +
//				"event_time timestamp," +
//
//				"PRIMARY KEY((edit_id), year, month, day, hour)," +
//				") WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC);"
//				);
		session.execute("CREATE TABLE IF NOT EXISTS master_dataset.edits("+ 
				//				"edit_uuid UUID," + 
				"edit_id INT," +
				"edit_minor BOOLEAN," +
				"edit_patrolled BOOLEAN," +
				"edit_length MAP<TEXT, INT>," +
				"edit_revision MAP<TEXT, INT>," +

	            "common_server_url TEXT," +
	            "common_server_name TEXT," +
	            "common_server_script_path TEXT," +
	            "common_server_wiki TEXT," +

				"common_event_type TEXT," +
				"common_event_namespace INT," +
				"common_event_user TEXT," +
				"common_event_bot BOOLEAN," +
				"common_event_comment TEXT," +
				"common_event_title TEXT," +

				"year INT," +
				"month INT," +
				"day INT," +
				"hour INT," +
				"event_time TIMESTAMP," +

				"PRIMARY KEY((edit_id), year, month, day, hour)," +
				") WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC);"
				);

	}

	public void populateFrom(String cassandraSeedHostname, String inputFile) {
		SparkConf conf = new SparkConf();
		conf.setMaster("spark://tomato:7077");
		conf.setAppName("wikitrends-migrate-master");
		conf.set("spark.cassandra.connection.host", cassandraSeedHostname);

		try (JavaSparkContext sc = new JavaSparkContext(conf);) {

			JavaRDD<JsonObject> oldMasterDataset = sc.textFile(inputFile)
					.map(l -> new JsonParser().parse(l).getAsJsonObject());

			JavaRDD<EditType> edits = oldMasterDataset.filter(change -> {
				String type = change.get("type").getAsString();
				return "edit".equals(type) || "new".equals(type);
			}).map(change -> parseEditFromJSON(change));

			CassandraJavaUtil.javaFunctions(edits).writerBuilder("master_dataset", "edits", mapToRow(EditType.class))
					.saveToCassandra();

			 JavaRDD<LogType> logs = oldMasterDataset.filter(change -> {
					String type = change.get("type").getAsString();
					return "log".equals(type);
				}).map(change -> parseLogFromJSON(change));
			 
			 CassandraJavaUtil.javaFunctions(logs)
			 .writerBuilder("master_dataset", "logs", mapToRow(LogType.class))
			 .saveToCassandra();
		}
	}

	private LogType parseLogFromJSON(JsonObject obj) {
		LogType lt;
		String log_params = obj.has("log_params") ? obj.get("log_params").toString() : null;

		Integer id = obj.has("id") && !obj.get("id").isJsonNull() ? obj.get("id").getAsInt() : null;

		String log_type = obj.has("log_type") ? obj.get("log_type").getAsString() : null;

		lt = new LogType(obj.get("server_url").getAsString(), obj.get("server_name").getAsString(),
				obj.get("server_script_path").getAsString(), obj.get("wiki").getAsString(),
				obj.get("type").getAsString(), obj.get("namespace").getAsInt(), obj.get("user").getAsString(),
				obj.get("bot").getAsBoolean(), obj.get("comment").getAsString(), obj.get("title").getAsString(),
				new DateTime(obj.get("timestamp").getAsLong() * 1000L).toDate(), UUID.randomUUID(), id,
				obj.get("log_id").getAsInt(), obj.get("log_action").getAsString(), log_type, log_params,
				obj.get("log_action_comment").getAsString());
		return lt;
	}

	private EditType parseEditFromJSON(JsonObject obj) {
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

	/**
	 * Entry point
	 * 
	 * @param args
	 *            cassandra seed node address and JSON input file to migrate.
	 */
	public static void main(String[] args) {

		if (args.length < 2) {
			System.err.println(
					"Usage: java -cp <CLASSPATH> br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager CREATE|POPULATE <seed_address>");
			System.exit(1);
		}

		String operation = args[0];
		String seedNode = args[1];

		CassandraMasterDatasetManager manager = new CassandraMasterDatasetManager();
		
		switch (operation) {
		case "CREATE":
			try (Cluster cluster = Cluster.builder().addContactPoints(seedNode).build();
					Session session = cluster.newSession();) {
				manager.createTables(session);
			}
			break;
		case "POPULATE":
			if (args.length < 2) {
				System.err.println(
						"Usage: java -cp <CLASSPATH> br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager POPULATE <seed_address> <input_file>");
				System.exit(1);
			}
			String inputFile = args[2];
			manager.populateFrom(seedNode, inputFile);
			break;
		default:
			System.err.println("Unsupported operation. Choose CREATE OR POPULATE");
			break;
		}

	}
}
