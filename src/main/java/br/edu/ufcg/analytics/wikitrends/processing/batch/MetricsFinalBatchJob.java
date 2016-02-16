package br.edu.ufcg.analytics.wikitrends.processing.batch;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowToTuple;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaRDD;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.batchview.types.KeyValuePair;
import scala.Tuple2;

public class MetricsFinalBatchJob extends AbstractFinalBatchJob {
	
	private static final long serialVersionUID = -8968582683538025373L;
	

	public MetricsFinalBatchJob(Configuration configuration) {
		super(configuration, BatchViewID.PARTIAL_METRICS, BatchViewID.FINAL_METRICS);
	}
	
	@Override
	public void process() {
		JavaRDD<KeyValuePair> rdd = javaFunctions(getJavaSparkContext())
		.cassandraTable(getKeyspace(), getInputBatchViewID(), mapRowToTuple(String.class, Long.class))
		.select("name", "value")
		.mapToPair(row -> new Tuple2<String, Long>(row._1, row._2))
		.reduceByKey((a,b) -> a+b)
		.map( tuple -> new KeyValuePair(getOutputBatchViewID(), tuple._1, tuple._2));
		
		CassandraJavaUtil.javaFunctions(rdd)
		.writerBuilder(getKeyspace(), getOutputBatchViewID(), mapToRow(KeyValuePair.class))
		.saveToCassandra();
	}
	
}
