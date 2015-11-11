package br.edu.ufcg.analytics.wikitrends.api.controller.beans;

public class RankingRow {
	
	private final String key;
	private final String value;

	public RankingRow(String key, String value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}
	
	public String getValue() {
		return value;
	}

}
