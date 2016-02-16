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
	public void createAll(Session session) {

		createMasterDatasetKeyspace(session);

		createChangesTable(session);

		createLogsTable(session);

		createEditsTable(session);

	}

	public void createEditsTable(Session session) {
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

				"PRIMARY KEY((year, month, day, hour), nonce));"
				);
	}

	public void createLogsTable(Session session) {
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

				"PRIMARY KEY((year, month, day, hour), nonce));"
				);
	}

	public void createChangesTable(Session session) {
		session.execute("CREATE TABLE IF NOT EXISTS master_dataset.changes(" +
				"nonce UUID," +
				"year INT," +
				"month INT," +
				"day INT," +
				"hour INT," +
				"event_timestamp TIMESTAMP," +
				"content TEXT," +

				"PRIMARY KEY((year, month, day, hour), nonce));"
		);
	}

	public void createMasterDatasetKeyspace(Session session) {
		session.execute("CREATE KEYSPACE IF NOT EXISTS master_dataset WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
	}

	/**
	 * @param session Opened {@link Session} to a cassandra DB.
	 */
	public void dropAll(Session session) {

		session.execute("DROP KEYSPACE IF EXISTS master_dataset");
	}
	
	public void dropTable(Session session, String table) {
		session.execute("DROP TABLE IF EXISTS master_dataset." + table);
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

		if (args.length != 2) {
			System.err.println(
					"Usage: java -cp <CLASSPATH> br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager CREATE|DROP <EDITS|LOGS|CHANGES|ALL>");
			System.err.println(
					"       or");
			System.err.println(
					"		java -cp <CLASSPATH> br.edu.ufcg.analytics.wikitrends.storage.raw.CassandraMasterDatasetManager POPULATE <SOURCE FILE>");
			System.exit(1);
		}

		String operation = args[0];
		
		String[] seeds = System.getProperty("spark.cassandra.connection.host").split(",");

		CassandraMasterDatasetManager manager = new CassandraMasterDatasetManager();

		switch (operation) {
		case "CREATE":
			try (Cluster cluster = Cluster.builder().addContactPoints(seeds).build();
					Session session = cluster.newSession();) {
				manager.createMasterDatasetKeyspace(session);
				String table = args[1];
				
				switch(table) {
				case("EDITS"):
					manager.createEditsTable(session);
					break;
				case("LOGS"):
					manager.createLogsTable(session);
					break;
				case("CHANGES"):
					manager.createChangesTable(session);
					break;
				default:
					manager.createAll(session);
					break;
				}
			}
			break;
		case "DROP":
			try (Cluster cluster = Cluster.builder().addContactPoints(seeds).build();
					Session session = cluster.newSession();) {
				String table = args[1];
				switch(table) {
				case("EDITS"):
					manager.dropTable(session, "edits");
					break;
				case("LOGS"):
					manager.dropTable(session, "logs");
					break;
				case("CHANGES"):
					manager.dropTable(session, "changes");
					break;
				default:
					manager.dropAll(session);
					break;
				}
			}
			break;
		case "POPULATE":
			manager.populate(args[1]);
			break;
		default:
			System.err.println("Unsupported operation. Choose CREATE, DROP or POPULATE");
			break;
		}

	}
}
