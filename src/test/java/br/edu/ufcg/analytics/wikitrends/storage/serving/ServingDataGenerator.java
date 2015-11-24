package br.edu.ufcg.analytics.wikitrends.storage.serving;

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

import br.edu.ufcg.analytics.wikitrends.storage.serving.types.AbsoluteValuesShot;
import br.edu.ufcg.analytics.wikitrends.storage.serving.types.TopClass;

public class ServingDataGenerator {
	
	private JavaSparkContext sc;
	
	public ServingDataGenerator(JavaSparkContext sc) {
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
																origin+(i*100000)
																);
        	
        	avs.setEvent_time(new DateTime(2014, 5, 23, i+1, i+2).toDate());
        	
        	listAbsoluteValues.add(avs);
        }

        JavaRDD<AbsoluteValuesShot> absoluteValuesRDD = sc.parallelize(listAbsoluteValues);
               
        CassandraJavaUtil.javaFunctions(absoluteValuesRDD)
        	.writerBuilder("batch_views", "absolute_values", mapToRow(AbsoluteValuesShot.class))
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
        
        DateTime dt21 = new DateTime(2013, 4, 3, 7, 0);
        DateTime dt22 = new DateTime(2013, 4, 3, 8, 0);
        DateTime dt23 = new DateTime(2013, 4, 4, 7, 0);
        DateTime dt24 = new DateTime(2013, 4, 4, 8, 0);
        
        TopClass te1 = new TopClass(m1);
        te1.setEvent_time(dt21.toDate());
        
        TopClass te2 = new TopClass(m2);
        te2.setEvent_time(dt22.toDate());
        
        TopClass te3 = new TopClass(m3);
        te1.setEvent_time(dt23.toDate());
        
        TopClass te4 = new TopClass(m4);
        te4.setEvent_time(dt24.toDate());
        
        List<TopClass> topEditors = Arrays.asList(
                te1, te2, te3, te4
        );

        JavaRDD<TopClass> topEditorsRDD = sc.parallelize(topEditors);

        CassandraJavaUtil.javaFunctions(topEditorsRDD)
        		.writerBuilder("batch_views", "top_editors", mapToRow(TopClass.class))
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
        
        DateTime dt21 = new DateTime(2013, 4, 3, 7, 0);
        DateTime dt22 = new DateTime(2013, 4, 3, 8, 0);
        DateTime dt23 = new DateTime(2013, 4, 4, 7, 0);
        DateTime dt24 = new DateTime(2013, 4, 4, 8, 0);
        
        TopClass ti1 = new TopClass(m1);
        ti1.setEvent_time(dt21.toDate());
        
        TopClass ti2 = new TopClass(m2);
        ti2.setEvent_time(dt22.toDate());
        
        TopClass ti3 = new TopClass(m3);
        ti1.setEvent_time(dt23.toDate());
        
        TopClass ti4 = new TopClass(m4);
        ti4.setEvent_time(dt24.toDate());
        
        List<TopClass> topIdioms = Arrays.asList(
                ti1, ti2, ti3, ti4
        );

        JavaRDD<TopClass> topIdiomsRDD = sc.parallelize(topIdioms);
        
        CassandraJavaUtil.javaFunctions(topIdiomsRDD)
				.writerBuilder("batch_views", "top_idioms", mapToRow(TopClass.class))
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
        
        DateTime dt21 = new DateTime(2013, 4, 3, 7, 0);
        DateTime dt22 = new DateTime(2013, 4, 3, 8, 0);
        DateTime dt23 = new DateTime(2013, 4, 4, 7, 0);
        DateTime dt24 = new DateTime(2013, 4, 4, 8, 0);
        
        TopClass tp1 = new TopClass(m1);
        tp1.setEvent_time(dt21.toDate());
        
        TopClass tp2 = new TopClass(m2);
        tp2.setEvent_time(dt22.toDate());
        
        TopClass tp3 = new TopClass(m3);
        tp1.setEvent_time(dt23.toDate());
        
        TopClass tp4 = new TopClass(m4);
        tp4.setEvent_time(dt24.toDate());
        
        List<TopClass> topPages = Arrays.asList(
                tp1, tp2, tp3, tp4
        );

        JavaRDD<TopClass> topPagesRDD = sc.parallelize(topPages);
        
        CassandraJavaUtil.javaFunctions(topPagesRDD)
				.writerBuilder("batch_views", "top_pages", mapToRow(TopClass.class))
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
        
        DateTime dt21 = new DateTime(2013, 4, 3, 7, 0);
        DateTime dt22 = new DateTime(2013, 4, 3, 8, 0);
        DateTime dt23 = new DateTime(2013, 4, 4, 7, 0);
        DateTime dt24 = new DateTime(2013, 4, 4, 8, 0);
        
        TopClass tcp1 = new TopClass(m1);
        tcp1.setEvent_time(dt21.toDate());
        
        TopClass tcp2 = new TopClass(m2);
        tcp2.setEvent_time(dt22.toDate());
        
        TopClass tcp3 = new TopClass(m3);
        tcp1.setEvent_time(dt23.toDate());
        
        TopClass tcp4 = new TopClass(m4);
        tcp4.setEvent_time(dt24.toDate());
        
        List<TopClass> topContentPages = Arrays.asList(
                tcp1, tcp2, tcp3, tcp4
        );

        JavaRDD<TopClass> topContentPagesRDD = sc.parallelize(topContentPages);
        
        CassandraJavaUtil.javaFunctions(topContentPagesRDD)
				.writerBuilder("batch_views", "top_content_pages", mapToRow(TopClass.class))
				.saveToCassandra();
    }
	
}
