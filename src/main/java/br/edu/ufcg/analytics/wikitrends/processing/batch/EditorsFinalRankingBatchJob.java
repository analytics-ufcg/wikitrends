package br.edu.ufcg.analytics.wikitrends.processing.batch;

import org.apache.commons.configuration.Configuration;

public class EditorsFinalRankingBatchJob extends AbstractFinalRankingBatchJob {
	
	private static final long serialVersionUID = -307773374341420488L;

	public EditorsFinalRankingBatchJob(Configuration configuration) {
		super(configuration, BatchViewID.EDITORS_PARTIAL_RANKINGS, BatchViewID.EDITORS_FINAL_RANKING, "distinct_editors_count");
	}
}
