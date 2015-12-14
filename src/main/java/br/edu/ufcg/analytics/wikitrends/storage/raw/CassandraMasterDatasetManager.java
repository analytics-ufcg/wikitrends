package br.edu.ufcg.analytics.wikitrends.storage.raw;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.io.Serializable;
import java.util.UUID;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import br.edu.ufcg.analytics.wikitrends.storage.CassandraManager;
import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditChange;
import br.edu.ufcg.analytics.wikitrends.storage.raw.types.LogChange;
import br.edu.ufcg.analytics.wikitrends.storage.raw.types.RawWikimediaChange;

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

		session.execute("CREATE TABLE IF NOT EXISTS master_dataset.changes(" +
				"nonce UUID," +
				"year INT," +
				"month INT," +
				"day INT," +
				"hour INT," +
				"event_timestamp TIMESTAMP," +
				"content TEXT," +

				"PRIMARY KEY((nonce), year, month, day, hour)," +
				") WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC);"
		);

		session.execute("CREATE TABLE IF NOT EXISTS master_dataset.logs(" +
				"nonce UUID," +
				"id INT," +
				"log_id INT," +
				"log_action TEXT," +
				"log_type TEXT," +
				"log_params TEXT," +
				"log_action_comment TEXT," +

	            "server_url TEXT," +
	            "server_name TEXT," +
	            "server_script_path TEXT," +
	            "wiki TEXT," +

				"type TEXT," +
				"namespace INT," +
				"user TEXT," +
				"bot BOOLEAN," +
				"comment TEXT," +
				"title TEXT," +

				"year INT," +
				"month INT," +
				"day INT," +
				"hour INT," +
				"event_timestamp TIMESTAMP," +

				"PRIMARY KEY((nonce), year, month, day, hour)," +
				") WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC);"
				);

		session.execute("CREATE TABLE IF NOT EXISTS master_dataset.edits("+ 
				"nonce UUID," + 
				"id INT," +
				"minor BOOLEAN," +
				"patrolled BOOLEAN," +
				"length MAP<TEXT, INT>," +
				"revision MAP<TEXT, INT>," +

	            "server_url TEXT," +
	            "server_name TEXT," +
	            "server_script_path TEXT," +
	            "wiki TEXT," +

				"type TEXT," +
				"namespace INT," +
				"user TEXT," +
				"bot BOOLEAN," +
				"comment TEXT," +
				"title TEXT," +

				"year INT," +
				"month INT," +
				"day INT," +
				"hour INT," +
				"event_timestamp TIMESTAMP," +

				"PRIMARY KEY((nonce), year, month, day, hour)," +
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
					.map(l -> {
						JsonObject jsonObject = new JsonParser().parse(l).getAsJsonObject();
						jsonObject.addProperty("uuid", UUID.randomUUID().toString());
						return jsonObject;
					});
			
			JavaRDD<RawWikimediaChange> changes = oldMasterDataset.
					map(change -> RawWikimediaChange.parseRawWikimediaChange(change));

		    CassandraJavaUtil.javaFunctions(changes).
	    	writerBuilder("master_dataset", "changes", mapToRow(RawWikimediaChange.class)).
	    	saveToCassandra();

			JavaRDD<EditChange> edits = oldMasterDataset.filter(change -> {
				String type = change.get("type").getAsString();
				return "edit".equals(type) || "new".equals(type);
			}).map(change -> parseEditFromJSON(change));

			CassandraJavaUtil.javaFunctions(edits).writerBuilder("master_dataset", "edits", mapToRow(EditChange.class))
			.saveToCassandra();

			JavaRDD<LogChange> logs = oldMasterDataset.filter(change -> {
				String type = change.get("type").getAsString();
				return "log".equals(type);
			}).map(change -> parseLogFromJSON(change));

			CassandraJavaUtil.javaFunctions(logs)
			.writerBuilder("master_dataset", "logs", mapToRow(LogChange.class))
			.saveToCassandra();
		}

	}

	private LogChange parseLogFromJSON(JsonObject obj) {
		return LogChange.parseLogChange(obj);
	}

	private EditChange parseEditFromJSON(JsonObject obj) {
		return EditChange.parseEditChange(obj);
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
