package br.edu.ufcg.analytics.wikitrends.storage.results;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.results.types.ResultAbsoluteValuesShot;
import br.edu.ufcg.analytics.wikitrends.storage.results.types.ResultTopContentPage;
import br.edu.ufcg.analytics.wikitrends.storage.results.types.ResultTopEditor;
import br.edu.ufcg.analytics.wikitrends.storage.results.types.ResultTopIdiom;
import br.edu.ufcg.analytics.wikitrends.storage.results.types.ResultTopPage;

public class ResultsDataGenerator {
	
	private JavaSparkContext sc;
	
	public ResultsDataGenerator(JavaSparkContext sc) {
		this.sc = sc;
	}
	
	public void generateAbsoluteValuesData() {
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
        	.writerBuilder("results", "absolute_values", mapToRow(ResultAbsoluteValuesShot.class))
        	.saveToCassandra();
    }
	
	public void generateResultingTopEditorsData() {
        
		ResultTopEditor rte1 = new ResultTopEditor("john_1", 2);
        ResultTopEditor rte2 = new ResultTopEditor("john_2", 0);
        ResultTopEditor rte3 = new ResultTopEditor("john_3", 4);
        ResultTopEditor rte4 = new ResultTopEditor("john_4", 3);
        ResultTopEditor rte5 = new ResultTopEditor("john_5", 10);
        
        List<ResultTopEditor> resultingTopEditors = Arrays.asList(
                rte1, rte2, rte3, rte4, rte5
        );

        JavaRDD<ResultTopEditor> resultingTopEditorsRDD = sc.parallelize(resultingTopEditors);

        CassandraJavaUtil.javaFunctions(resultingTopEditorsRDD)
        		.writerBuilder("results", "top_editor", mapToRow(ResultTopEditor.class))
        		.saveToCassandra();

    }
	
	public void generateResultingTopIdiomsData() {
        ResultTopIdiom rti1 = new ResultTopIdiom("en", 2);
        ResultTopIdiom rti2 = new ResultTopIdiom("fr", 1);
        ResultTopIdiom rti3 = new ResultTopIdiom("pt", 4);
        ResultTopIdiom rti4 = new ResultTopIdiom("de", 3);
        ResultTopIdiom rti5 = new ResultTopIdiom("ru", 10);

        List<ResultTopIdiom> resultingTopIdioms = Arrays.asList(
                rti1, rti2, rti3, rti4, rti5
        );

        JavaRDD<ResultTopIdiom> topIdiomsRDD = sc.parallelize(resultingTopIdioms);
        
        CassandraJavaUtil.javaFunctions(topIdiomsRDD)
				.writerBuilder("results", "top_idiom", mapToRow(ResultTopIdiom.class))
				.saveToCassandra();
        
    }
	
	public void generateResultingTopPagesData() {
        ResultTopPage rtp1 = new ResultTopPage("page_1", 2);
        ResultTopPage rtp2 = new ResultTopPage("page_2", 0);
        ResultTopPage rtp3 = new ResultTopPage("page_3", 4);
        ResultTopPage rtp4 = new ResultTopPage("page_4", 3);
        ResultTopPage rtp5 = new ResultTopPage("page_5", 10);

        List<ResultTopPage> resultingTopPages = Arrays.asList(
                rtp1, rtp2, rtp3, rtp4, rtp5
        );

        JavaRDD<ResultTopPage> topPagesRDD = sc.parallelize(resultingTopPages);
        
        CassandraJavaUtil.javaFunctions(topPagesRDD)
				.writerBuilder("results", "top_page", mapToRow(ResultTopPage.class))
				.saveToCassandra();
    }
	
	public void generateResultingTopContentPagesData() {
        ResultTopContentPage rtcp1 = new ResultTopContentPage("content_page_1", 2);
        ResultTopContentPage rtcp2 = new ResultTopContentPage("content_page_2", 0);
        ResultTopContentPage rtcp3 = new ResultTopContentPage("content_page_3", 4);
        ResultTopContentPage rtcp4 = new ResultTopContentPage("content_page_4", 3);
        ResultTopContentPage rtcp5 = new ResultTopContentPage("content_page_5", 10);
        
        List<ResultTopContentPage> topContentPages = Arrays.asList(
                rtcp1, rtcp2, rtcp3, rtcp4, rtcp5
        );

        JavaRDD<ResultTopContentPage> resultingTopContentPagesRDD = sc.parallelize(topContentPages);
        
        CassandraJavaUtil.javaFunctions(resultingTopContentPagesRDD)
				.writerBuilder("results", "top_content_page", mapToRow(ResultTopContentPage.class))
				.saveToCassandra();
    }
	
}
