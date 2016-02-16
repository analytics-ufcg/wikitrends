package br.edu.ufcg.analytics.wikitrends.processing.batch;

import org.apache.commons.configuration.Configuration;

public class ContentPagesFinalRankingBatchJob extends AbstractFinalRankingBatchJob {
	
	private static final long serialVersionUID = 5181147901979329455L;
	
	public ContentPagesFinalRankingBatchJob(Configuration configuration)  {
		super(configuration, BatchViewID.CONTENT_PAGES_PARTIAL_RANKINGS, BatchViewID.CONTENT_PAGES_FINAL_RANKING,  "distinct_content_pages_count");
	}
}
