package br.edu.ufcg.analytics.wikitrends.processing.batch2;

public enum ProcessResultID {
	TOP_IDIOMS ("top_idioms"),
	TOP_EDITORS ("top_editors"),
	TOP_PAGES ("top_pages"),
	TOP_CONTENT_PAGES ("top_content_pages"),
	ABSOLUTE_VALUES ("absolute_values");
	
	private String ID; 
	
	ProcessResultID(String pResultID) {
		this.setID(pResultID);
	}

	public String getID() {
		return ID;
	}

	public void setID(String iD) {
		ID = iD;
	}
}
