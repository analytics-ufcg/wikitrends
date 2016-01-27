package br.edu.ufcg.analytics.wikitrends.storage.serving2.types;

import java.io.Serializable;

public class KeyValuePair implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7067611819442680877L;
	private String name;
	private Long value;
	public KeyValuePair(String name, Long value) {
		super();
		this.name = name;
		this.value = value;
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
		return "KeyValuePair [name=" + name + ", value=" + value + "]";
	}
	
	

}
