package br.edu.ufcg.analytics.wikitrends.processing.batch;

import org.apache.commons.configuration.Configuration;

public class IdiomsFinalRankingBatchJob extends AbstractFinalRankingBatchJob {
	
	private static final long serialVersionUID = 6811359470576431827L;

	public IdiomsFinalRankingBatchJob(Configuration configuration) {
		super(configuration, BatchViewID.IDIOMS_PARTIAL_RANKINGS, BatchViewID.IDIOMS_FINAL_RANKING, "distinct_idioms_count");
	}

}
