package br.edu.ufcg.analytics.wikitrends.processing.batch;

import java.time.LocalDateTime;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaRDD;

import br.edu.ufcg.analytics.wikitrends.storage.master.types.EditChange;

public class ContentPagesPartialRankingBatchJob extends PagesPartialRankingBatchJob {

	private static final long serialVersionUID = 5005439419731611631L;

	public ContentPagesPartialRankingBatchJob(Configuration configuration) {
		super(configuration, BatchViewID.CONTENT_PAGES_PARTIAL_RANKINGS);
	}

	@Override
	public JavaRDD<EditChange> read(LocalDateTime currentTime) {
		return super.read(currentTime).filter(edit -> edit.getNamespace() == 0);
	}

}
