package br.edu.ufcg.analytics.wikitrends.storage.serving2;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.ResultAbsoluteValuesShot;
import br.edu.ufcg.analytics.wikitrends.storage.serving2.types.TopResult;

public class BatchViews2DataGenerator {
	
	private JavaSparkContext sc;
	
	public BatchViews2DataGenerator(JavaSparkContext sc) {
		this.sc = sc;
	}
	
	public void generateResultingAbsoluteValuesData() {
        Long all_edits = 3758062L;
        Long minor_edits =	600606L;
        Long average_size = 209L;
        
        Long distinct_pages_count = 15234324L;
        Integer distinct_editors_count = 398345;
        Integer distinct_servers_count = 294;
        
        Long origin = 1444077595L; // in milliseconds (?)
        
//        Long batch_e_time = 1392315L; // in milliseconds (?)
//        Integer total_executor_cores = 4;
//        Long input_size = 5145694870L; // in bytes (?)
//        
        List<ResultAbsoluteValuesShot> listAbsoluteValues = new ArrayList<ResultAbsoluteValuesShot>();
        for(int i = 0; i < 6; i++) {
        	ResultAbsoluteValuesShot ravs = new ResultAbsoluteValuesShot(
        											"absolute_values",
        											all_edits+(i*10000000L),
        											minor_edits+(i*1000000L),
        											average_size+(i*10L),
        											distinct_pages_count + (i * 10000L),
        											distinct_editors_count + (i * 10),
													distinct_servers_count + (i * 100),
													origin+(i*100000L)
												);
        	
        	listAbsoluteValues.add(ravs);
        }

        JavaRDD<ResultAbsoluteValuesShot> resultsAbsoluteValuesRDD = sc.parallelize(listAbsoluteValues);
               
        CassandraJavaUtil.javaFunctions(resultsAbsoluteValuesRDD)
        	.writerBuilder("batch_views2", "absolute_values", mapToRow(ResultAbsoluteValuesShot.class))
        	.saveToCassandra();
    }
	
	public void generateResultingTopEditorsData() {
        
		TopResult rte1 = new TopResult("top_editors", "john_1", 2L);
        TopResult rte2 = new TopResult("top_editors", "john_2", 0L);
        TopResult rte3 = new TopResult("top_editors", "john_3", 4L);
        TopResult rte4 = new TopResult("top_editors", "john_4", 3L);
        TopResult rte5 = new TopResult("top_editors", "john_5", 10L);
        
        List<TopResult> resultingTopEditors = Arrays.asList(
                rte1, rte2, rte3, rte4, rte5
        );

        JavaRDD<TopResult> resultingTopEditorsRDD = sc.parallelize(resultingTopEditors);

        CassandraJavaUtil.javaFunctions(resultingTopEditorsRDD)
        		.writerBuilder("batch_views2", "top_editors", mapToRow(TopResult.class))
        		.saveToCassandra();

    }
	
	public void generateResultingTopIdiomsData() {
        TopResult rti1 = new TopResult("top_idioms", "en", 2L);
        TopResult rti2 = new TopResult("top_idioms", "fr", 1L);
        TopResult rti3 = new TopResult("top_idioms", "pt", 4L);
        TopResult rti4 = new TopResult("top_idioms", "de", 3L);
        TopResult rti5 = new TopResult("top_idioms", "ru", 10L);

        List<TopResult> resultingTopIdioms = Arrays.asList(
                rti1, rti2, rti3, rti4, rti5
        );

        JavaRDD<TopResult> topIdiomsRDD = sc.parallelize(resultingTopIdioms);
        
        CassandraJavaUtil.javaFunctions(topIdiomsRDD)
				.writerBuilder("batch_views2", "top_idioms", mapToRow(TopResult.class))
				.saveToCassandra();
        
    }
	
	public void generateResultingTopPagesData() {
        TopResult rtp1 = new TopResult("top_pages", "page_1", 2L);
        TopResult rtp2 = new TopResult("top_pages", "page_2", 0L);
        TopResult rtp3 = new TopResult("top_pages", "page_3", 4L);
        TopResult rtp4 = new TopResult("top_pages", "page_4", 3L);
        TopResult rtp5 = new TopResult("top_pages", "page_5", 10L);

        List<TopResult> resultingTopPages = Arrays.asList(
                rtp1, rtp2, rtp3, rtp4, rtp5
        );

        JavaRDD<TopResult> topPagesRDD = sc.parallelize(resultingTopPages);
        
        CassandraJavaUtil.javaFunctions(topPagesRDD)
				.writerBuilder("batch_views2", "top_pages", mapToRow(TopResult.class))
				.saveToCassandra();
    }
	
	public void generateResultingTopContentPagesData() {
        TopResult rtcp1 = new TopResult("top_content_pages", "content_page_1", 2L);
        TopResult rtcp2 = new TopResult("top_content_pages", "content_page_2", 0L);
        TopResult rtcp3 = new TopResult("top_content_pages", "content_page_3", 4L);
        TopResult rtcp4 = new TopResult("top_content_pages", "content_page_4", 3L);
        TopResult rtcp5 = new TopResult("top_content_pages", "content_page_5", 10L);
        
        List<TopResult> topContentPages = Arrays.asList(
                rtcp1, rtcp2, rtcp3, rtcp4, rtcp5
        );

        JavaRDD<TopResult> resultingTopContentPagesRDD = sc.parallelize(topContentPages);
        
        CassandraJavaUtil.javaFunctions(resultingTopContentPagesRDD)
				.writerBuilder("batch_views2", "top_content_pages", mapToRow(TopResult.class))
				.saveToCassandra();
    }
	
}
