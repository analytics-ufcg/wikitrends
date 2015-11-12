package br.edu.ufcg.analytics.wikitrends.data;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;
import java.util.UUID;

import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import br.edu.ufcg.analytics.wikitrends.datatypes.EditType;
import br.edu.ufcg.analytics.wikitrends.datatypes.LogType;

public class DataGenerator {
	private JavaSparkContext sc;

	public DataGenerator(JavaSparkContext sc) {
		this.sc = sc;
	}
	
	public void populateDatabase(String path) {
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());
		
		try (Session session = connector.openSession()){
			@SuppressWarnings("resource")
			Scanner scan = new Scanner(new File(path));
			EditType et;
			LogType lt;

			while(scan.hasNextLine()){
		        String line = scan.nextLine();
		        
		        JsonObject obj = new JsonParser().parse(line).getAsJsonObject();

		        //System.out.println(obj.toString());
				
		        if(obj.get("type").getAsString().equals("edit")) {
		        	JsonObject j_obj1 = obj.has("length") ? obj.get("length").getAsJsonObject() : null;
					JsonObject j_obj2 = obj.has("revision") ? obj.get("revision").getAsJsonObject() : null;
					
					HashMap<String, Integer> m1 = null, m2 = null;
					if(j_obj1 != null) {
						m1 = new HashMap<String, Integer>();
						m1.put("new", j_obj1.has("new") ? j_obj1.get("new").getAsInt() : null);
						m1.put("old", j_obj1.has("old") ? j_obj1.get("old").getAsInt() : null);
					}
					if(j_obj2 != null) {
						m2 = new HashMap<String, Integer>();
						m2.put("new", j_obj2.has("new") ? j_obj2.get("new").getAsInt() : null);
						m2.put("old", j_obj2.has("old") ? j_obj2.get("old").getAsInt() : null);
					}
					
					Boolean patrolled = obj.has("patrolled") ? obj.get("patrolled").getAsBoolean() : null;
					
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
		        	
		        	//System.out.println(et);
		        	
		        	CassandraJavaUtil.javaFunctions(sc.parallelize(Arrays.asList(et)))
		        		.writerBuilder("master_dataset", "edits", mapToRow(EditType.class))
		        		.saveToCassandra();
		        }
		        else if(obj.get("type").getAsString().equals("log")) {
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
}
