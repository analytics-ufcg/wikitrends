package br.edu.ufcg.analytics.wikitrends.data;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.io.File;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Scanner;
import java.util.UUID;

import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

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

		        System.out.println(obj.toString());
				
		        if(obj.get("type").getAsString().equals("edit")) {
		        	JsonObject j_obj1 = obj.get("length").getAsJsonObject();
					JsonObject j_obj2 = obj.get("revision").getAsJsonObject();
					
					HashMap<String, Integer> m1 = new HashMap<String, Integer>();
					HashMap<String, Integer> m2 = new HashMap<String, Integer>();
					
					m1.put("new", j_obj1.get("new").getAsInt());
					m1.put("old", j_obj1.get("old").getAsInt());
					
					m2.put("new", j_obj2.get("new").getAsInt());
					m2.put("old", j_obj2.get("old").getAsInt());
		        	
					DateTime dateTime = new DateTime( obj.get("timestamp").getAsLong() ).withZone(DateTimeZone.getDefault());
					Date event_time = dateTime.toDate();
					
					Boolean patrolled;
					
					try {
						patrolled = obj.get("patrolled").getAsBoolean();
					}
					catch(NullPointerException npe) {
						patrolled = null;
					}
					
					
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
        							   event_time,
        							   UUID.randomUUID(),
        							   obj.get("id").getAsInt(),
        							   obj.get("minor").getAsBoolean(),
        							   patrolled,
        							   m1, 
        							   m2);
		        	
		        	System.out.println(et);
		        	
		        	CassandraJavaUtil.javaFunctions(sc.parallelize(Arrays.asList(et)))
		        		.writerBuilder("master_dataset", "edits", mapToRow(EditType.class))
		        		.saveToCassandra();
		        }
		        else if(obj.get("type").getAsString().equals("log")) {
					DateTime dateTime = new DateTime( obj.get("timestamp").getAsLong() ).withZone(DateTimeZone.getDefault());
					Date event_time = dateTime.toDate();
					
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
	    							   event_time,
	    							   UUID.randomUUID(),
	    							   obj.get("log_id").getAsInt(),
	    							   obj.get("log_type").getAsString(),
	    							   obj.get("log_action").getAsString(),
	    							   obj.get("log_params").getAsString(), 
	    							   obj.get("log_actions_comment").getAsString());
		        	
		        	System.out.println(lt);
		        	
		        	CassandraJavaUtil.javaFunctions(sc.parallelize(Arrays.asList(lt)))
			        		.writerBuilder("master_dataset", "logs", mapToRow(LogType.class))
			        		.saveToCassandra();
		        }
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
