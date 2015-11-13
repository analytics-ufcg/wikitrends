package br.edu.ufcg.analytics.wikitrends.storage.raw;

import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Session;

public class TablesGenerator {

	private Session session;

	public TablesGenerator(Session session) {
		this.session = session;
	}

	public TablesGenerator(JavaSparkContext sc) {
		// TODO Auto-generated constructor stub
	}

	public void generate() {

		session.execute("DROP KEYSPACE IF EXISTS master_dataset");

		session.execute("CREATE KEYSPACE master_dataset WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

		session.execute("CREATE TABLE IF NOT EXISTS master_dataset." +
				"logs" +
				"(log_uuid UUID," +
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
				"common_event_namespace TEXT," +
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
		session.execute("CREATE TABLE IF NOT EXISTS master_dataset." +
				"edits" +
				"(edit_uuid UUID," + 
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
				"common_event_namespace TEXT," +
				"common_event_user TEXT," +
				"common_event_bot BOOLEAN," +
				"common_event_comment TEXT," +
				"common_event_title TEXT," +

				"year INT," +
				"month INT," +
				"day INT," +
				"hour INT," +
				"event_time TIMESTAMP," +

				"PRIMARY KEY((edit_uuid), year, month, day, hour)," +
				") WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC);"
			);
	}

}
