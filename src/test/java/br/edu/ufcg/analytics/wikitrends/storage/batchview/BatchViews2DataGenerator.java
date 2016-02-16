package br.edu.ufcg.analytics.wikitrends.storage.batchview;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.batchview.types.KeyValuePair;
import br.edu.ufcg.analytics.wikitrends.storage.batchview.types.RankingEntry;

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
        List<KeyValuePair> listAbsoluteValues = new ArrayList<KeyValuePair>();
        for(int i = 0; i < 6; i++) {
        	listAbsoluteValues.add(new KeyValuePair("absolute_values", "all_edits", all_edits+(i*10000000L)));
        	listAbsoluteValues.add(new KeyValuePair("absolute_values", "minor_edits", minor_edits+(i*1000000L)));
        	listAbsoluteValues.add(new KeyValuePair("absolute_values", "average_size", average_size+(i*10L)));
        	listAbsoluteValues.add(new KeyValuePair("absolute_values", "distinct_pages_count", distinct_pages_count + (i * 10000L)));
        	listAbsoluteValues.add(new KeyValuePair("absolute_values", "distinct_editors_count", distinct_editors_count + (i * 10L)));
        	listAbsoluteValues.add(new KeyValuePair("absolute_values", "distinct_servers_count", distinct_servers_count + (i * 100L)));
        	listAbsoluteValues.add(new KeyValuePair("absolute_values", "origin", origin+(i*100000L)));
        }

        JavaRDD<KeyValuePair> resultsAbsoluteValuesRDD = sc.parallelize(listAbsoluteValues);
               
        CassandraJavaUtil.javaFunctions(resultsAbsoluteValuesRDD)
        	.writerBuilder("batch_views2", "absolute_values", mapToRow(KeyValuePair.class))
        	.saveToCassandra();
    }
	
	public void generateResultingTopEditorsData() {
        
		RankingEntry rte1 = new RankingEntry("top_editors", 3L, "john_1", 2L);
        RankingEntry rte2 = new RankingEntry("top_editors", 4L, "john_2", 0L);
        RankingEntry rte3 = new RankingEntry("top_editors", 1L, "john_3", 4L);
        RankingEntry rte4 = new RankingEntry("top_editors", 2L, "john_4", 3L);
        RankingEntry rte5 = new RankingEntry("top_editors", 0L, "john_5", 10L);
        
        List<RankingEntry> resultingTopEditors = Arrays.asList(
                rte1, rte2, rte3, rte4, rte5
        );

        JavaRDD<RankingEntry> resultingTopEditorsRDD = sc.parallelize(resultingTopEditors);

        CassandraJavaUtil.javaFunctions(resultingTopEditorsRDD)
        		.writerBuilder("batch_views2", "top_editors", mapToRow(RankingEntry.class))
        		.saveToCassandra();

    }
	
	public void generateResultingTopIdiomsData() {
        RankingEntry rti1 = new RankingEntry("top_idioms", 3L, "en", 2L);
        RankingEntry rti2 = new RankingEntry("top_idioms", 4L, "fr", 1L);
        RankingEntry rti3 = new RankingEntry("top_idioms", 1L, "pt", 4L);
        RankingEntry rti4 = new RankingEntry("top_idioms", 2L, "de", 3L);
        RankingEntry rti5 = new RankingEntry("top_idioms", 0L, "ru", 10L);

        List<RankingEntry> resultingTopIdioms = Arrays.asList(
                rti1, rti2, rti3, rti4, rti5
        );

        JavaRDD<RankingEntry> topIdiomsRDD = sc.parallelize(resultingTopIdioms);
        
        CassandraJavaUtil.javaFunctions(topIdiomsRDD)
				.writerBuilder("batch_views2", "top_idioms", mapToRow(RankingEntry.class))
				.saveToCassandra();
        
    }
	
	public void generateResultingTopPagesData() {
        RankingEntry rtp1 = new RankingEntry("top_pages", 3L, "page_1", 2L);
        RankingEntry rtp2 = new RankingEntry("top_pages", 4L, "page_2", 0L);
        RankingEntry rtp3 = new RankingEntry("top_pages", 1L, "page_3", 4L);
        RankingEntry rtp4 = new RankingEntry("top_pages", 2L, "page_4", 3L);
        RankingEntry rtp5 = new RankingEntry("top_pages", 0L, "page_5", 10L);

        List<RankingEntry> resultingTopPages = Arrays.asList(
                rtp1, rtp2, rtp3, rtp4, rtp5
        );

        JavaRDD<RankingEntry> topPagesRDD = sc.parallelize(resultingTopPages);
        
        CassandraJavaUtil.javaFunctions(topPagesRDD)
				.writerBuilder("batch_views2", "top_pages", mapToRow(RankingEntry.class))
				.saveToCassandra();
    }
	
	public void generateResultingTopContentPagesData() {
        RankingEntry rtcp1 = new RankingEntry("top_content_pages", 3L, "content_page_1", 2L);
        RankingEntry rtcp2 = new RankingEntry("top_content_pages", 4L, "content_page_2", 0L);
        RankingEntry rtcp3 = new RankingEntry("top_content_pages", 1L, "content_page_3", 4L);
        RankingEntry rtcp4 = new RankingEntry("top_content_pages", 2L, "content_page_4", 3L);
        RankingEntry rtcp5 = new RankingEntry("top_content_pages", 0L, "content_page_5", 10L);
        
        List<RankingEntry> topContentPages = Arrays.asList(
                rtcp1, rtcp2, rtcp3, rtcp4, rtcp5
        );

        JavaRDD<RankingEntry> resultingTopContentPagesRDD = sc.parallelize(topContentPages);
        
        CassandraJavaUtil.javaFunctions(resultingTopContentPagesRDD)
				.writerBuilder("batch_views2", "top_content_pages", mapToRow(RankingEntry.class))
				.saveToCassandra();
    }
	
}
