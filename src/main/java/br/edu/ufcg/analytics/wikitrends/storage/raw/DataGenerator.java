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
						   obj.get("namespace").getAsInt(),
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
		JsonObject length = obj.get("length").getAsJsonObject();
		
		HashMap<String, Long> lengthMap = new HashMap<>(2);
		if(!length.get("new").isJsonNull()){
			lengthMap.put("new", length.get("new").getAsLong());
		}
		if(!length.get("old").isJsonNull()){
			lengthMap.put("old", length.get("old").getAsLong());
		}
		
		JsonObject review = obj.get("revision").getAsJsonObject();
		
		HashMap<String, Long>  revisionMap = new HashMap<>(2);
		if(!review.get("new").isJsonNull()){
			revisionMap.put("new", review.get("new").getAsLong());
		}
		if(!review.get("old").isJsonNull()){
			revisionMap.put("old", review.get("old").getAsLong());
		}
		
		Boolean patrolled = obj.has("patrolled") && !obj.get("patrolled").isJsonNull() ? obj.get("patrolled").getAsBoolean() : null;

		return new EditType(obj.get("server_url").getAsString(),
						   obj.get("server_name").getAsString(),
						   obj.get("server_script_path").getAsString(),
						   obj.get("wiki").getAsString(),
						   obj.get("type").getAsString(),
						   obj.get("namespace").getAsInt(),
						   obj.get("user").getAsString(),
						   obj.get("bot").getAsBoolean(),
						   obj.get("comment").getAsString(),
						   obj.get("title").getAsString(),
						   new DateTime(obj.get("timestamp").getAsLong() * 1000L).toDate(),
						   UUID.randomUUID(),
						   obj.get("id").getAsInt(),
						   obj.get("minor").getAsBoolean(),
						   patrolled,
						   lengthMap, 
						   revisionMap);
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
			.filter(change -> {
				String type = change.get("type").getAsString();
				return "edit".equals(type) || "new".equals(type);
			})
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
