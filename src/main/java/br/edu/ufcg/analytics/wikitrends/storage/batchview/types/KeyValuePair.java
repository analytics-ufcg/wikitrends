package br.edu.ufcg.analytics.wikitrends.storage.serving2.types;

import java.io.Serializable;

public class KeyValuePair implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3736647272504204181L;
	private String id;
	private String name;
	private Long value;
	public KeyValuePair(String id, String name, Long value) {
		super();
		this.id = id;
		this.name = name;
		this.value = value;
	}
	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}
	/**
	 * @param id the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}
	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}
	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}
	/**
	 * @return the value
	 */
	public Long getValue() {
		return value;
	}
	/**
	 * @param value the value to set
	 */
	public void setValue(Long value) {
		this.value = value;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "KeyValuePair [id=" + id + ", name=" + name + ", value=" + value + "]";
	}
	
	

}
