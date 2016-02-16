package br.edu.ufcg.analytics.wikitrends.processing;

/**
 * The string that identifies the job status
 * into the batch_views.status table, that is used
 * to keep track on the execution times of the jobs.
 * 
 * @author Guilherme Gadelha
 *
 */
public enum JobStatusID {
	TOP_IDIOMS_BATCH_1 ("top_idioms_batch_1"),
	TOP_EDITORS_BATCH_1 ("top_editors_batch_1"),
	TOP_PAGES_BATCH_1 ("top_pages_batch_1"),
	TOP_CONTENT_PAGES_BATCH_1 ("top_content_pages_batch_1"),
	ABS_VALUES_BATCH_1 ("abs_values_batch_1"),
	
	TOP_IDIOMS_BATCH_2 ("top_idioms_batch_2"),
	TOP_EDITORS_BATCH_2 ("top_editors_batch_2"),
	TOP_PAGES_BATCH_2 ("top_pages_batch_2"),
	TOP_CONTENT_PAGES_BATCH_2 ("top_content_pages_batch_2"),
	ABS_VALUES_BATCH_2 ("abs_values_batch_2");
	
	private String status_id;

	JobStatusID(String status_id) {
		this.status_id = status_id;
	}

	public String getStatus_id() {
		return status_id;
	}

}
