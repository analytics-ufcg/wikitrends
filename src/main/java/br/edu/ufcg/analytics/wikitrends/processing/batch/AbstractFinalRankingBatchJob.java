package br.edu.ufcg.analytics.wikitrends.processing.batch;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowToTuple;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.Arrays;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaRDD;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.batchview.types.KeyValuePair;
import br.edu.ufcg.analytics.wikitrends.storage.batchview.types.RankingEntry;
import scala.Tuple2;

public abstract class AbstractFinalRankingBatchJob extends AbstractFinalBatchJob {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4549243218651124137L;
	
	private String finalRankingsTable;
	private BatchViewID metricsID;
	private String distinctCountMetricID;

	
	public AbstractFinalRankingBatchJob(Configuration configuration, BatchViewID inputStatusID, BatchViewID outputStatusID, String distinctCountMetricID) {
		super(configuration, inputStatusID, outputStatusID);
		this.distinctCountMetricID = distinctCountMetricID;
		this.finalRankingsTable =  "final_rankings"; //FIXME get from conf
		this.metricsID = BatchViewID.FINAL_METRICS;
	}

	
	/**
	 * @return the metricsTable
	 */
	public String getMetricsTable() {
		return metricsID.toString();
	}

	/**
	 * @return the rankingsTable
	 */
	public String getFinalRankingsTable() {
		return finalRankingsTable;
	}


	@Override
	public void process() {
		JavaRDD<RankingEntry> fullRanking = computeFullRankingFromPartial();
		CassandraJavaUtil.javaFunctions(fullRanking)
			.writerBuilder(getKeyspace(), getFinalRankingsTable(), mapToRow(RankingEntry.class))
			.saveToCassandra();
		
		JavaRDD<KeyValuePair> distinctRDD = getJavaSparkContext().parallelize(Arrays.asList(new KeyValuePair(getMetricsTable(), getDistinctCountMetricID(), fullRanking.count())));
		
		CassandraJavaUtil.javaFunctions(distinctRDD)
		.writerBuilder(getKeyspace(), getMetricsTable(), mapToRow(KeyValuePair.class))
		.saveToCassandra();

	}


	/**
	 * Compute final ranking for the given table/process. 
	 * 
	 *  If final table is not empty it
	 * takes the values from the final table and merge them with the new values from the
	 * new hour.
	 * 
	 *  If the final table is empty it takes the values calculated from the current hour and
	 *  put them directly in the final table. 
	 * 
	 * @param tableName
	 * @return rdd of topResults
	 */
	private JavaRDD<RankingEntry> computeFullRankingFromPartial() {
		return javaFunctions(getJavaSparkContext())
				.cassandraTable(getKeyspace(), getInputBatchViewID(), mapRowToTuple(String.class, Long.class))
				.select("name", "count")
				.mapToPair(row -> new Tuple2<String, Long>(row._1, row._2))
				.reduceByKey((a,b) -> a+b)
				.mapToPair( pair -> pair.swap() )
				.sortByKey(false)
				.zipWithIndex()
				.map( tuple -> new RankingEntry(getOutputBatchViewID(), tuple._2, tuple._1._2, tuple._1._1));
	}

	private String getDistinctCountMetricID(){
		return distinctCountMetricID;
	}

}
