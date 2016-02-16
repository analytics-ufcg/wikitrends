package br.edu.ufcg.analytics.wikitrends.processing.batch;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.time.LocalDateTime;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.batchview.types.RankingEntryPerHour;
import br.edu.ufcg.analytics.wikitrends.storage.master.types.EditChange;
import scala.Tuple2;

public abstract class AbstractPartialRankingBatchJob extends AbstractPartialBatchJob {

	/**
	 * 
	 */
	private static final long serialVersionUID = 726816049103810916L;

	public AbstractPartialRankingBatchJob(Configuration configuration, BatchViewID processStartTimeStatusId) {
		super(configuration, processStartTimeStatusId);
	}

	@Override
	protected void process(LocalDateTime currentTime) {
		JavaRDD<EditChange> wikipediaEdits = read(currentTime);

		JavaPairRDD<String, Long> userRDD = wikipediaEdits
				.mapToPair(edit -> new Tuple2<String, Long>(getRankingKey(edit), 1L));

		JavaRDD<RankingEntryPerHour> userRanking = userRDD.reduceByKey((a, b) -> a + b)
				.map(edit -> new RankingEntryPerHour(edit._1, edit._2, currentTime));

		CassandraJavaUtil.javaFunctions(userRanking)
				.writerBuilder(getKeyspace(), getBatchViewID().toString(), mapToRow(RankingEntryPerHour.class)).saveToCassandra();
	}

	protected abstract JavaRDD<EditChange> read(LocalDateTime currentTime);

	protected abstract String getRankingKey(EditChange edit);

}
