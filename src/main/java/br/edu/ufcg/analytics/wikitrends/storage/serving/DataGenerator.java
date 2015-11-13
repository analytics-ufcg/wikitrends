package br.edu.ufcg.analytics.wikitrends.storage.serving;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.serving.types.AbsoluteValuesShot;
import br.edu.ufcg.analytics.wikitrends.storage.serving.types.TopClass;

public class DataGenerator {
	
	private JavaSparkContext sc;
	private String className;

	public DataGenerator(JavaSparkContext sc, String className) {
		this.sc = sc;
		this.className = className;
	}

	public void generateData() {
		if(className == "top_editors") {
			this.generateTopEditorsData();
		}
		else if(className == "top_pages") {
			this.generateTopPagesData();
		}
		else if(className == "top_content_pages") {
			this.generateTopContentPagesData();
		}
		else if(className == "top_idioms") {
			this.generateTopIdiomsData();
		}
		else if(className == "absolute_values") {
			this.generateAbsoluteData();
		}
	}
	
	private void generateAbsoluteData() {
        Integer all_edits =	3758062;
        Integer minor_edits =	600606;
        Integer average_size = 209;
        Integer distinct_pages = 1550572;
        Integer distinct_editors = 471502;
        Integer distinct_servers = 280;
        Long origin = 1444077595L; // in milliseconds (?)
        Long batch_e_time = 1392315L; // in milliseconds (?)
        Integer total_executor_cores = 4;
        Long input_size = 5145694870L; // in bytes (?)
        
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
        SimpleDateFormat hourFormatter = new SimpleDateFormat("HH", Locale.ENGLISH);
        
        List<AbsoluteValuesShot> listAbsoluteValues = new ArrayList<AbsoluteValuesShot>();
        for(int i = 0; i < 6; i++) {
        	Date event_time = new Date();
        	listAbsoluteValues.add(new AbsoluteValuesShot(
        									i,
        									dateFormatter.format(event_time),
        									hourFormatter.format(event_time),
        									all_edits+i*10000000,
        									minor_edits+i*1000000,
        									average_size+i,
        									distinct_pages+i,
        									distinct_editors+i,
        									distinct_servers+i,
        									origin+(i*100000),
        									batch_e_time+(i*1000L),
        									total_executor_cores,
        									input_size+i*10,
        									event_time)
        			);
        	try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }

        JavaRDD<AbsoluteValuesShot> absoluteValuesRDD = sc.parallelize(listAbsoluteValues);
               
        CassandraJavaUtil.javaFunctions(absoluteValuesRDD)
        	.writerBuilder("batch_views", "absolute_values", mapToRow(AbsoluteValuesShot.class))
        	.saveToCassandra();
    }
	
	private void generateTopEditorsData() {
        final Map<String, Integer> m1;
        {
        	m1 = new HashMap<String, Integer>();
        	m1.put("john_1", 2);
        	m1.put("john_2", 0);
        	m1.put("john_3", 4);
        	m1.put("john_4", 3);
        	m1.put("john_5", 10);
        };
        final Map<String, Integer> m2;
        {
        	m2 = new HashMap<String, Integer>();
        	m2.put("john_1", 2);
        	m2.put("john_2", 0);
        	m2.put("john_3", 4);
        	m2.put("john_4", 3);
        	m2.put("john_5", 10);
        };
        final Map<String, Integer> m3;
        {
        	m3 = new HashMap<String, Integer>();
        	m3.put("john_1", 2);
        	m3.put("john_2", 0);
        	m3.put("john_3", 4);
        	m3.put("john_4", 3);
        	m3.put("john_5", 10);
        };
        final Map<String, Integer> m4;
        {
        	m4 = new HashMap<String, Integer>();
        	m4.put("john_1", 2);
        	m4.put("john_2", 0);
        	m4.put("john_3", 4);
        	m4.put("john_4", 3);
        	m4.put("john_5", 10);
        };
        
        Date dt11 = null, dt12 = null, dt13 = null, dt14 = null, dt21 = null, dt22 = null, dt23 = null, dt24 = null;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
        SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.ENGLISH);
        try{
	        dt11 = formatter.parse("2013-04-04");
	        dt12 = formatter.parse("2013-04-03");
	        dt13 = formatter.parse("2013-04-04");
	        dt14 = formatter.parse("2013-04-03");
	        
	        dt21 = formatter2.parse("2013-04-04 07:02");
	        dt22 = formatter2.parse("2013-04-04 07:01");
	        dt23 = formatter2.parse("2013-04-03 07:02");
	        dt24 = formatter2.parse("2013-04-03 07:01");
        }
        catch(Exception e) {
        	System.out.println(e.getMessage());
        }
        
        // Prepare the products hierarchy
        List<TopClass> topEditors = Arrays.asList(
                new TopClass(1, dt11, 7, dt21, m1),
                new TopClass(2, dt12, 7, dt22, m2),
                new TopClass(3, dt13, 7, dt23, m3),
                new TopClass(4, dt14, 7, dt24, m4)
        );

        JavaRDD<TopClass> topEditorsRDD = sc.parallelize(topEditors);

        CassandraJavaUtil.javaFunctions(topEditorsRDD)
        		.writerBuilder("batch_views", "top_editors", mapToRow(TopClass.class))
        		.saveToCassandra();

    }
	
	private void generateTopIdiomsData() {
        final Map<String, Integer> m1;
        {
        	m1 = new HashMap<String, Integer>();
        	m1.put("en", 2);
        	m1.put("fr", 1);
        	m1.put("pt", 4);
        	m1.put("de", 3);
        	m1.put("ru", 10);
        };
        final Map<String, Integer> m2;
        {
        	m2 = new HashMap<String, Integer>();
        	m2.put("en", 2);
        	m2.put("fr", 1);
        	m2.put("pt", 4);
        	m2.put("de", 3);
        	m2.put("ru", 10);
        };
        final Map<String, Integer> m3;
        {
        	m3 = new HashMap<String, Integer>();
        	m3.put("en", 2);
        	m3.put("fr", 1);
        	m3.put("pt", 4);
        	m3.put("de", 3);
        	m3.put("ru", 10);
        };
        final Map<String, Integer> m4;
        {
        	m4 = new HashMap<String, Integer>();
        	m4.put("en", 2);
        	m4.put("fr", 1);
        	m4.put("pt", 4);
        	m4.put("de", 3);
        	m4.put("ru", 10);
        };
        
        Date dt11 = null, dt12 = null, dt13 = null, dt14 = null, dt21 = null, dt22 = null, dt23 = null, dt24 = null;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
        SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.ENGLISH);
        try{
	        dt11 = formatter.parse("2013-04-04");
	        dt12 = formatter.parse("2013-04-03");
	        dt13 = formatter.parse("2013-04-04");
	        dt14 = formatter.parse("2013-04-03");
	        
	        dt21 = formatter2.parse("2013-04-04 07:02");
	        dt22 = formatter2.parse("2013-04-04 07:01");
	        dt23 = formatter2.parse("2013-04-03 07:02");
	        dt24 = formatter2.parse("2013-04-03 07:01");
        }
        catch(Exception e) {
        	System.out.println(e.getMessage());
        }
        
        // Prepare the products hierarchy
        List<TopClass> topIdioms = Arrays.asList(
                new TopClass(1, dt11, 7, dt21, m1),
                new TopClass(2, dt12, 7, dt22, m2),
                new TopClass(3, dt13, 7, dt23, m3),
                new TopClass(4, dt14, 7, dt24, m4)
        );

        JavaRDD<TopClass> topIdiomsRDD = sc.parallelize(topIdioms);
        
        CassandraJavaUtil.javaFunctions(topIdiomsRDD)
				.writerBuilder("batch_views", "top_idioms", mapToRow(TopClass.class))
				.saveToCassandra();
    }
	
	private void generateTopPagesData() {
        final Map<String, Integer> m1;
        {
        	m1 = new HashMap<String, Integer>();
        	m1.put("page_1", 2);
        	m1.put("page_2", 0);
        	m1.put("page_3", 4);
        	m1.put("page_4", 3);
        	m1.put("page_5", 10);
        };
        final Map<String, Integer> m2;
        {
        	m2 = new HashMap<String, Integer>();
        	m2.put("page_1", 2);
        	m2.put("page_2", 0);
        	m2.put("page_3", 4);
        	m2.put("page_4", 3);
        	m2.put("page_5", 10);
        };
        final Map<String, Integer> m3;
        {
        	m3 = new HashMap<String, Integer>();
        	m3.put("page_1", 2);
        	m3.put("page_2", 0);
        	m3.put("page_3", 4);
        	m3.put("page_4", 3);
        	m3.put("page_5", 10);
        };
        final Map<String, Integer> m4;
        {
        	m4 = new HashMap<String, Integer>();
        	m4.put("page_1", 2);
        	m4.put("page_2", 0);
        	m4.put("page_3", 4);
        	m4.put("page_4", 3);
        	m4.put("page_5", 10);
        };
        
        Date dt11 = null, dt12 = null, dt13 = null, dt14 = null, dt21 = null, dt22 = null, dt23 = null, dt24 = null;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
        SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.ENGLISH);
        try{
	        dt11 = formatter.parse("2013-04-04");
	        dt12 = formatter.parse("2013-04-03");
	        dt13 = formatter.parse("2013-04-04");
	        dt14 = formatter.parse("2013-04-03");
	        
	        dt21 = formatter2.parse("2013-04-04 07:02");
	        dt22 = formatter2.parse("2013-04-04 07:01");
	        dt23 = formatter2.parse("2013-04-03 07:02");
	        dt24 = formatter2.parse("2013-04-03 07:01");
        }
        catch(Exception e) {
        	System.out.println(e.getMessage());
        }
        
        // Prepare the products hierarchy
        List<TopClass> topEditors = Arrays.asList(
                new TopClass(1, dt11, 7, dt21, m1),
                new TopClass(2, dt12, 7, dt22, m2),
                new TopClass(3, dt13, 7, dt23, m3),
                new TopClass(4, dt14, 7, dt24, m4)
        );

        JavaRDD<TopClass> topPagesRDD = sc.parallelize(topEditors);
               
        CassandraJavaUtil.javaFunctions(topPagesRDD)
			.writerBuilder("batch_views", "top_pages", mapToRow(TopClass.class))
			.saveToCassandra();
    }
	
	private void generateTopContentPagesData() {
        final Map<String, Integer> m1;
        {
        	m1 = new HashMap<String, Integer>();
        	m1.put("content_page_1", 2);
        	m1.put("content_page_2", 0);
        	m1.put("content_page_3", 4);
        	m1.put("content_page_4", 3);
        	m1.put("content_page_5", 10);
        };
        final Map<String, Integer> m2;
        {
        	m2 = new HashMap<String, Integer>();
        	m2.put("content_page_1", 2);
        	m2.put("content_page_2", 0);
        	m2.put("content_page_3", 4);
        	m2.put("content_page_4", 3);
        	m2.put("content_page_5", 10);
        };
        final Map<String, Integer> m3;
        {
        	m3 = new HashMap<String, Integer>();
        	m3.put("content_page_1", 2);
        	m3.put("content_page_2", 0);
        	m3.put("content_page_3", 4);
        	m3.put("content_page_4", 3);
        	m3.put("content_page_5", 10);
        };
        final Map<String, Integer> m4;
        {
        	m4 = new HashMap<String, Integer>();
        	m4.put("content_page_1", 2);
        	m4.put("content_page_2", 0);
        	m4.put("content_page_3", 4);
        	m4.put("content_page_4", 3);
        	m4.put("content_page_5", 10);
        };
        
        Date dt11 = null, dt12 = null, dt13 = null, dt14 = null, dt21 = null, dt22 = null, dt23 = null, dt24 = null;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
        SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.ENGLISH);
        try{
	        dt11 = formatter.parse("2013-04-04");
	        dt12 = formatter.parse("2013-04-03");
	        dt13 = formatter.parse("2013-04-04");
	        dt14 = formatter.parse("2013-04-03");
	        
	        dt21 = formatter2.parse("2013-04-04 07:02");
	        dt22 = formatter2.parse("2013-04-04 07:01");
	        dt23 = formatter2.parse("2013-04-03 07:02");
	        dt24 = formatter2.parse("2013-04-03 07:01");
        }
        catch(Exception e) {
        	System.out.println(e.getMessage());
        }
        
        // Prepare the products hierarchy
        List<TopClass> topContentPages = Arrays.asList(
                new TopClass(1, dt11, 7, dt21, m1),
                new TopClass(2, dt12, 7, dt22, m2),
                new TopClass(3, dt13, 7, dt23, m3),
                new TopClass(4, dt14, 7, dt24, m4)
        );
        
        System.out.println("topContentPages: " + topContentPages.toString());

        JavaRDD<TopClass> topContentPagesRDD = sc.parallelize(topContentPages);
               
        CassandraJavaUtil.javaFunctions(topContentPagesRDD)
			.writerBuilder("batch_views", "top_content_pages", mapToRow(TopClass.class))
			.saveToCassandra();
    }
	
}
