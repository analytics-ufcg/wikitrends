package br.edu.ufcg.analytics.wikitrends.storage.raw;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import br.edu.ufcg.analytics.wikitrends.storage.CassandraManager;
import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditType;
import br.edu.ufcg.analytics.wikitrends.storage.raw.types.LogType;

/**
 * @author Ricardo Araújo Santos - ricoaraujosantos@gmail.com
 *
 */
public class CassandraMasterDatasetManager extends CassandraManager implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6066687152165846375L;

	/**
	 * Default empty constructor
	 */
	public CassandraMasterDatasetManager() {
		// TODO Default empty constructor
	}

	/**
	 * @param session Opened {@link Session} to a cassandra DB.
	 */
	public void createTables(Session session) {

		session.execute("CREATE KEYSPACE IF NOT EXISTS master_dataset WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

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

	/**
	 * @param session Opened {@link Session} to a cassandra DB.
	 */
	public void dropTables(Session session) {

		session.execute("DROP KEYSPACE IF EXISTS master_dataset");
	}

	public void populate(String source) {
		
		SparkConf conf = new SparkConf();
		try(JavaSparkContext sc = new JavaSparkContext(conf);){
			JavaRDD<JsonObject> oldMasterDataset = sc.textFile(source)
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
		return LogType.parseLogType(obj);
	}

	private EditType parseEditFromJSON(JsonObject obj) {
		return EditType.parseEditType(obj);
	}

	/**
	 * Entry point
	 * 
	 * @param args
	 *            cassandra seed node address and JSON input file to migrate.
	 */
	public static void main(String[] args) {

		if (args.length < 1) {
			System.err.println(
					"Usage: java -cp <CLASSPATH> br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager CREATE|POPULATE");
			System.exit(1);
		}

		String operation = args[0];
		String seed = System.getProperty("spark.cassandra.connection.host");

		CassandraMasterDatasetManager manager = new CassandraMasterDatasetManager();

		switch (operation) {
		case "CREATE":
			try (Cluster cluster = Cluster.builder().addContactPoints(seed).build();
					Session session = cluster.newSession();) {
				manager.createTables(session);
			}
			break;
		case "DROP":
			try (Cluster cluster = Cluster.builder().addContactPoints(seed).build();
					Session session = cluster.newSession();) {
				manager.dropTables(session);
			}
			break;
		case "POPULATE":
			if (args.length < 2) {
				System.err.println(
						"Usage: java -cp <CLASSPATH> br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager POPULATE <source file>");
				System.exit(1);
			}

			String source = args[1];

			manager.populate(source);

			break;
		default:
			System.err.println("Unsupported operation. Choose CREATE, DROP or POPULATE");
			break;
		}

	}
}
