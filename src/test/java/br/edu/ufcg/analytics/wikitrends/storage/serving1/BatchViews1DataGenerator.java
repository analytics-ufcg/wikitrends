package br.edu.ufcg.analytics.wikitrends.storage.serving1;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.serving1.types.AbsoluteValuesShot;
import br.edu.ufcg.analytics.wikitrends.storage.serving1.types.TopClass;

public class BatchViews1DataGenerator {
	
	private JavaSparkContext sc;
	
	public BatchViews1DataGenerator(JavaSparkContext sc) {
		this.sc = sc;
	}
	
	public void generateAbsoluteData() {
        Long all_edits = 3758062L;
        Long minor_edits =	600606L;
        Long average_size = 209L;
        
        Set<String> distinct_pages = new HashSet<String>();
        Set<String> distinct_editors = new HashSet<String>();
        Set<String> distinct_servers = new HashSet<String>();
        
        Long origin = 1444077595L; // in milliseconds (?)
        
//        Long batch_e_time = 1392315L; // in milliseconds (?)
//        Integer total_executor_cores = 4;
//        Long input_size = 5145694870L; // in bytes (?)
//        
        List<AbsoluteValuesShot> listAbsoluteValues = new ArrayList<AbsoluteValuesShot>();
        for(int i = 0; i < 6; i++) {
        	Map<String, Long> m = new HashMap<String, Long>();
            m.put("all_edits", all_edits+i*10000000);
            m.put("minor_edits", minor_edits+i*1000000);
        	m.put("average_size", average_size+i);
            
        	distinct_pages.add("page"+String.valueOf(i));
        	distinct_editors.add("ed" + String.valueOf(i));
        	distinct_servers.add("server" + String.valueOf(i));
        	
        	AbsoluteValuesShot avs = new AbsoluteValuesShot(m, 
    														distinct_pages,
															distinct_editors,
															distinct_servers,
															origin+(i*100000),
															2014,
															5,
															23,
															i+1);
        	
        	listAbsoluteValues.add(avs);
        }

        JavaRDD<AbsoluteValuesShot> absoluteValuesRDD = sc.parallelize(listAbsoluteValues);
               
        CassandraJavaUtil.javaFunctions(absoluteValuesRDD)
        	.writerBuilder("batch_views1", "absolute_values", mapToRow(AbsoluteValuesShot.class))
        	.saveToCassandra();
    }
	
	public void generateTopEditorsData() {
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
        
        
        List<TopClass> topEditors = new ArrayList<TopClass>();
		for(int i=0; i < 5; i++) {
			DateTime dt21 = new DateTime(2013, 4, 3, 7+i, 0);
			DateTime dt22 = new DateTime(2013, 4, 3, 8+i, 0);
			DateTime dt23 = new DateTime(2013, 4, 4, 7+i, 0);
			DateTime dt24 = new DateTime(2013, 4, 4, 8+i, 0);

			TopClass ti1 = new TopClass(m1.keySet().toArray()[0].toString(), (long)m1.get(m1.keySet().toArray()[0]), dt21.getYear(), dt21.getMonthOfYear(), dt21.getDayOfWeek(), dt21.getHourOfDay());
	        
	        TopClass ti2 = new TopClass(m2.keySet().toArray()[1].toString(), (long)m2.get(m2.keySet().toArray()[1]), dt22.getYear(), dt22.getMonthOfYear(), dt22.getDayOfWeek(), dt22.getHourOfDay());
	       
	        TopClass ti3 = new TopClass(m3.keySet().toArray()[2].toString(), (long)m3.get(m3.keySet().toArray()[2]), dt23.getYear(), dt23.getMonthOfYear(), dt23.getDayOfWeek(), dt23.getHourOfDay());
	        
	        TopClass ti4 = new TopClass(m4.keySet().toArray()[3].toString(), (long)m4.get(m4.keySet().toArray()[3]), dt24.getYear(), dt24.getMonthOfYear(), dt24.getDayOfWeek(), dt24.getHourOfDay());
	        
	        topEditors.addAll(Arrays.asList(ti1, ti2, ti3, ti4));
        }
		
        JavaRDD<TopClass> topEditorsRDD = sc.parallelize(topEditors);

        CassandraJavaUtil.javaFunctions(topEditorsRDD)
        		.writerBuilder("batch_views1", "top_editors", mapToRow(TopClass.class))
        		.saveToCassandra();

    }
	
	public void generateTopIdiomsData() {
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
        
        
        List<TopClass> topIdioms = new ArrayList<TopClass>();
		for(int i=0; i < 5; i++) {
			DateTime dt21 = new DateTime(2013, 4, 3, 7+i, 0);
			DateTime dt22 = new DateTime(2013, 4, 3, 8+i, 0);
			DateTime dt23 = new DateTime(2013, 4, 4, 7+i, 0);
			DateTime dt24 = new DateTime(2013, 4, 4, 8+i, 0);
			
	        TopClass ti1 = new TopClass(m1.keySet().toArray()[0].toString(), (long)m1.get(m1.keySet().toArray()[0]), dt21.getYear(), dt21.getMonthOfYear(), dt21.getDayOfWeek(), dt21.getHourOfDay());
	        
	        TopClass ti2 = new TopClass(m2.keySet().toArray()[1].toString(), (long)m2.get(m2.keySet().toArray()[1]), dt22.getYear(), dt22.getMonthOfYear(), dt22.getDayOfWeek(), dt22.getHourOfDay());
	        
	        TopClass ti3 = new TopClass(m3.keySet().toArray()[2].toString(), (long)m3.get(m3.keySet().toArray()[2]), dt23.getYear(), dt23.getMonthOfYear(), dt23.getDayOfWeek(), dt23.getHourOfDay());
	        
	        TopClass ti4 = new TopClass(m4.keySet().toArray()[3].toString(), (long)m4.get(m4.keySet().toArray()[3]), dt24.getYear(), dt24.getMonthOfYear(), dt24.getDayOfWeek(), dt24.getHourOfDay());
	        
	        topIdioms.addAll(Arrays.asList(ti1, ti2, ti3, ti4));
        }

        JavaRDD<TopClass> topIdiomsRDD = sc.parallelize(topIdioms);
        
        CassandraJavaUtil.javaFunctions(topIdiomsRDD)
				.writerBuilder("batch_views1", "top_idioms", mapToRow(TopClass.class))
				.saveToCassandra();
        
    }
	
	public void generateTopPagesData() {
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
        
        List<TopClass> topPages = new ArrayList<TopClass>();
		for(int i=0; i < 5; i++) {
			DateTime dt21 = new DateTime(2013, 4, 3, 7+i, 0);
			DateTime dt22 = new DateTime(2013, 4, 3, 8+i, 0);
			DateTime dt23 = new DateTime(2013, 4, 4, 7+i, 0);
			DateTime dt24 = new DateTime(2013, 4, 4, 8+i, 0);
			
	        TopClass ti1 = new TopClass(m1.keySet().toArray()[0].toString(), (long)m1.get(m1.keySet().toArray()[0]), dt21.getYear(), dt21.getMonthOfYear(), dt21.getDayOfWeek(), dt21.getHourOfDay());
	        
	        TopClass ti2 = new TopClass(m2.keySet().toArray()[1].toString(), (long)m2.get(m2.keySet().toArray()[1]), dt22.getYear(), dt22.getMonthOfYear(), dt22.getDayOfWeek(), dt22.getHourOfDay());
	        
	        TopClass ti3 = new TopClass(m3.keySet().toArray()[2].toString(), (long)m3.get(m3.keySet().toArray()[2]), dt23.getYear(), dt23.getMonthOfYear(), dt23.getDayOfWeek(), dt23.getHourOfDay());
	        
	        TopClass ti4 = new TopClass(m4.keySet().toArray()[3].toString(), (long)m4.get(m4.keySet().toArray()[3]), dt24.getYear(), dt24.getMonthOfYear(), dt24.getDayOfWeek(), dt24.getHourOfDay());
	        
	        topPages.addAll(Arrays.asList(ti1, ti2, ti3, ti4));
        }
        

        JavaRDD<TopClass> topPagesRDD = sc.parallelize(topPages);
        
        CassandraJavaUtil.javaFunctions(topPagesRDD)
				.writerBuilder("batch_views1", "top_pages", mapToRow(TopClass.class))
				.saveToCassandra();
    }
	
	public void generateTopContentPagesData() {
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
        
        List<TopClass> topContentPages = new ArrayList<TopClass>();
		for(int i=0; i < 5; i++) {
			DateTime dt21 = new DateTime(2013, 4, 3, 7+i, 0);
			DateTime dt22 = new DateTime(2013, 4, 3, 8+i, 0);
			DateTime dt23 = new DateTime(2013, 4, 4, 7+i, 0);
			DateTime dt24 = new DateTime(2013, 4, 4, 8+i, 0);
			
	        TopClass ti1 = new TopClass(m1.keySet().toArray()[0].toString(), (long)m1.get(m1.keySet().toArray()[0]), dt21.getYear(), dt21.getMonthOfYear(), dt21.getDayOfWeek(), dt21.getHourOfDay());
	        
	        TopClass ti2 = new TopClass(m2.keySet().toArray()[1].toString(), (long)m2.get(m2.keySet().toArray()[1]), dt22.getYear(), dt22.getMonthOfYear(), dt22.getDayOfWeek(), dt22.getHourOfDay());
	        
	        TopClass ti3 = new TopClass(m3.keySet().toArray()[2].toString(), (long)m3.get(m3.keySet().toArray()[2]), dt23.getYear(), dt23.getMonthOfYear(), dt23.getDayOfWeek(), dt23.getHourOfDay());
	        
	        TopClass ti4 = new TopClass(m4.keySet().toArray()[3].toString(), (long)m4.get(m4.keySet().toArray()[3]), dt24.getYear(), dt24.getMonthOfYear(), dt24.getDayOfWeek(), dt24.getHourOfDay());
	        
	        topContentPages.addAll(Arrays.asList(ti1, ti2, ti3, ti4));
        }

        JavaRDD<TopClass> topContentPagesRDD = sc.parallelize(topContentPages);
        
        CassandraJavaUtil.javaFunctions(topContentPagesRDD)
				.writerBuilder("batch_views1", "top_content_pages", mapToRow(TopClass.class))
				.saveToCassandra();
    }
	
}
