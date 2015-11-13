package br.edu.ufcg.analytics.wikitrends.processing.batch;

import java.io.Serializable;

/**
 * Output formatter
 * 
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 */
public class BatchLayerOutput implements Serializable{

	private static final long serialVersionUID = 1595860019246486013L;
	
	private String key;
	private String value;

	/**
	 * Default constructor
	 * @param key
	 * @param value
	 */
	public BatchLayerOutput(String key, String value) {
		this.key = key;
		this.value = value;
	}
	
	public BatchLayerOutput(String key, Long countAllEdits) {
		this(key, countAllEdits.toString());
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return key + '\t' + value;
	}

}
