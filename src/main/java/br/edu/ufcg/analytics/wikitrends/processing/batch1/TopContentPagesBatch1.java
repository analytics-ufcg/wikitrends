package br.edu.ufcg.analytics.wikitrends.processing.batch1;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditChange;
import br.edu.ufcg.analytics.wikitrends.storage.serving1.types.TopClass;
import scala.Tuple2;

public class TopContentPagesBatch1 extends BatchLayer1Job {

	private static final long serialVersionUID = 5005439419731611631L;

	private String contentPagesTable;

	public TopContentPagesBatch1(Configuration configuration) {
		super(configuration);
		
		contentPagesTable = configuration.getString("wikitrends.batch.cassandra.table.contentpages");
	}

	public void process() {
		JavaRDD<EditChange> wikipediaEdits = read()
				.filter(edit -> edit.getServerName().endsWith("wikipedia.org"))
				.cache();
		
		JavaPairRDD<String, Integer> contentTitleRDD = wikipediaEdits
			.filter(edit -> "0".equals(edit.getNamespace()))
			.mapToPair( edit -> new Tuple2<String, Integer>(edit.getTitle(), 1));
		
		JavaRDD<TopClass> contentTitleRanking = transformToTopEntry(contentTitleRDD);
		
		CassandraJavaUtil.javaFunctions(contentTitleRanking)
			.writerBuilder(getBatchViewsKeyspace(), contentPagesTable, mapToRow(TopClass.class))
			.saveToCassandra();
		
		finalizeSparkContext();
	}
}
