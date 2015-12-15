package br.edu.ufcg.analytics.wikitrends.storage.serving2.types;

import java.io.Serializable;

public class TopResult implements Serializable {
	private static final long serialVersionUID = -4651390156724384971L;
	
	private String id;
	private String name;
	private Long count;

	public TopResult(String id, String name, Long count) {
		this.id = id;
	    this.name = name;
	    this.count = count;
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "TopResult [id=" + id + ", name=" + name + ", count=" + count + "]";
	}

}
