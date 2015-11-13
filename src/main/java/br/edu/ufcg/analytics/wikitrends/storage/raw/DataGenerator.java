package br.edu.ufcg.analytics.wikitrends.storage.raw;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;
import java.util.UUID;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditType;
import br.edu.ufcg.analytics.wikitrends.storage.raw.types.LogType;

public class DataGenerator implements Serializable{
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 6066687152165846375L;
	private String inputFile;

	public DataGenerator(String cassandraSeedHostname, String inputFile) {
		this.inputFile = inputFile;
	}
	
	private transient JavaSparkContext sc;

	public DataGenerator(JavaSparkContext sc) {
		this.sc = sc;
	}
	
	public void populateDatabase(String path) {
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());
		
		try (Session session = connector.openSession()){
			@SuppressWarnings("resource")
			Scanner scan = new Scanner(new File(path));
			LogType lt;

			while(scan.hasNextLine()){
		        String line = scan.nextLine();
		        
		        JsonObject obj = new JsonParser().parse(line).getAsJsonObject();

		        //System.out.println(obj.toString());
				
		        if(obj.get("type").getAsString().equals("edit")) {
		        	//System.out.println(et);
		        	
		        	CassandraJavaUtil.javaFunctions(sc.parallelize(Arrays.asList(parseEdit(obj))))
		        		.writerBuilder("master_dataset", "edits", mapToRow(EditType.class))
		        		.saveToCassandra();
		        }
		        else if(obj.get("type").getAsString().equals("log")) {
					lt = parseLog(obj);
		        	
		        	//System.out.println(lt);
		        	
		        	CassandraJavaUtil.javaFunctions(sc.parallelize(Arrays.asList(lt)))
			        		.writerBuilder("master_dataset", "logs", mapToRow(LogType.class))
			        		.saveToCassandra();
		        }
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("Database populated with success!!");
		}
	}

	private LogType parseLog(JsonObject obj) {
		LogType lt;
		String log_params = obj.has("log_params") ? obj.get("log_params").toString() : null;
		
		Integer id = obj.has("id") && !obj.get("id").isJsonNull() ? obj.get("id").getAsInt() : null;
		
		String log_type = obj.has("log_type") ? obj.get("log_type").getAsString() : null;
		
		lt = new LogType(obj.get("server_url").getAsString(),
						   obj.get("server_name").getAsString(),
						   obj.get("server_script_path").getAsString(),
						   obj.get("wiki").getAsString(),
						   obj.get("type").getAsString(),
						   obj.get("namespace").getAsString(),
						   obj.get("user").getAsString(),
						   obj.get("bot").getAsBoolean(),
						   obj.get("comment").getAsString(),
						   obj.get("title").getAsString(),
						   new DateTime(obj.get("timestamp").getAsLong() * 1000L).toDate(),
						   UUID.randomUUID(),
						   id,
						   obj.get("log_id").getAsInt(),
						   obj.get("log_action").getAsString(),
						   log_type,
						   log_params, 
						   obj.get("log_action_comment").getAsString());
		return lt;
	}

	private EditType parseEdit(JsonObject obj) {
		EditType et;
		JsonObject j_obj1 = obj.has("length") ? obj.get("length").getAsJsonObject() : null;
		JsonObject j_obj2 = obj.has("revision") ? obj.get("revision").getAsJsonObject() : null;
		
		HashMap<String, Integer> m1 = null, m2 = null;
		if(j_obj1 != null) {
			m1 = new HashMap<String, Integer>();
			if(j_obj1.has("new") && !j_obj1.get("new").isJsonNull()){
				m1.put("new", j_obj1.get("new").getAsInt());
			}
			if(j_obj1.has("old") && !j_obj1.get("old").isJsonNull()){
				m1.put("old", j_obj1.get("old").getAsInt());
			}
		}
		if(j_obj2 != null) {
			m2 = new HashMap<String, Integer>();
			if(j_obj2.has("new") && !j_obj2.get("new").isJsonNull()){
				m2.put("new", j_obj2.get("new").getAsInt());
			}
			if(j_obj2.has("old") && !j_obj2.get("old").isJsonNull()){
				m2.put("new", j_obj2.get("new").getAsInt());
				m2.put("old", j_obj2.get("old").getAsInt());
			}
		}
		
		Boolean patrolled = obj.has("patrolled") && !obj.get("patrolled").isJsonNull() ? obj.get("patrolled").getAsBoolean() : null;
		
		et = new EditType(obj.get("server_url").getAsString(),
						   obj.get("server_name").getAsString(),
						   obj.get("server_script_path").getAsString(),
						   obj.get("wiki").getAsString(),
						   obj.get("type").getAsString(),
						   obj.get("namespace").getAsString(),
						   obj.get("user").getAsString(),
						   obj.get("bot").getAsBoolean(),
						   obj.get("comment").getAsString(),
						   obj.get("title").getAsString(),
						   new DateTime(obj.get("timestamp").getAsLong() * 1000L).toDate(),
						   UUID.randomUUID(),
						   obj.get("id").getAsInt(),
						   obj.get("minor").getAsBoolean(),
						   patrolled,
						   m1, 
						   m2);
		return et;
	}

	public void run() {
		SparkConf conf = new SparkConf();
		conf.setAppName("wikitrends-migrate-master");
		conf.setMaster("spark://tomato:7077");
		conf.set("spark.cassandra.connection.host", "localhost");
		
		try(JavaSparkContext sc = new JavaSparkContext(conf);){

			JavaRDD<JsonObject> oldMasterDataset = sc.textFile(inputFile)
					.map(l -> new JsonParser().parse(l).getAsJsonObject());
			
			JavaRDD<EditType> edits = oldMasterDataset
			.filter(change -> !"log".equals(change.get("type").getAsString()))
			.map(change -> parseEdit(change));
			
			CassandraJavaUtil.javaFunctions(edits)
			.writerBuilder("master_dataset", "edits", mapToRow(EditType.class))
			.saveToCassandra();

//			JavaRDD<LogType> logs = oldMasterDataset
//			.filter(change -> "log".equals(change.get("type").getAsString()))
//			.map(change -> parseLog(change));
//
//			CassandraJavaUtil.javaFunctions(logs)
//			.writerBuilder("master_dataset", "logs", mapToRow(LogType.class))
//			.saveToCassandra();
		}
	}
		
		
}
