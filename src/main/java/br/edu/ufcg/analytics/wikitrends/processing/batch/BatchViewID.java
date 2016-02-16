package br.edu.ufcg.analytics.wikitrends.processing.batch;

import java.io.Serializable;

/**
 * The string that identifies the job status
 * into the batch_views.status table, that is used
 * to keep track on the execution times of the jobs.
 * 
 * @author Guilherme Gadelha
 *
 */
public enum BatchViewID implements Serializable {
	
	STATUS,
	
	IDIOMS_PARTIAL_RANKINGS,
	EDITORS_PARTIAL_RANKINGS,
	PAGES_PARTIAL_RANKINGS,
	CONTENT_PAGES_PARTIAL_RANKINGS,
	PARTIAL_METRICS,
	
	IDIOMS_FINAL_RANKING,
	EDITORS_FINAL_RANKING,
	PAGES_FINAL_RANKING,
	CONTENT_PAGES_FINAL_RANKING,
	FINAL_METRICS;
	
	@Override
	public String toString() {
		return name().toLowerCase();
	}

}
