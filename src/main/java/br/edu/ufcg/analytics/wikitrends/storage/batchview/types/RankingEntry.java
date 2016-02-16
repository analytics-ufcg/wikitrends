package br.edu.ufcg.analytics.wikitrends.storage.batchview.types;

import java.io.Serializable;

public class RankingEntry implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2591719259478944501L;
	/**
	 * 
	 */
	private String id;
	private Long position;
	private String name;
	private Long count;
	public RankingEntry(String id, Long position, String name, Long count) {
		super();
		this.id = id;
		this.name = name;
		this.position = position;
		this.count = count;
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
	 * @return the count
	 */
	public Long getCount() {
		return count;
	}
	/**
	 * @param count the count to set
	 */
	public void setCount(Long count) {
		this.count = count;
	}
	/**
	 * @return the position
	 */
	public Long getPosition() {
		return position;
	}
	/**
	 * @param position the position to set
	 */
	public void setPosition(Long position) {
		this.position = position;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "RankingEntry [id=" + id + ", name=" + name + ", position=" + position + ", count=" + count + "]";
	}

}
