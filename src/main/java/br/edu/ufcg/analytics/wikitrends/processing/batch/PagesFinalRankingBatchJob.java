package br.edu.ufcg.analytics.wikitrends.processing.batch;

import org.apache.commons.configuration.Configuration;

public class PagesFinalRankingBatchJob extends AbstractFinalRankingBatchJob {
	
	private static final long serialVersionUID = -8448922796164774655L;
	
	
	public PagesFinalRankingBatchJob(Configuration configuration) {
		super(configuration, BatchViewID.PAGES_PARTIAL_RANKINGS, BatchViewID.PAGES_FINAL_RANKING, "distinct_pages_count");
	}

}
