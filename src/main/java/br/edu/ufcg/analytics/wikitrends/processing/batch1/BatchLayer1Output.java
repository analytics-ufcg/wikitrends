package br.edu.ufcg.analytics.wikitrends.processing.batch1;

import java.io.Serializable;

/**
 * Output formatter
 * 
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 * @author Guilherme Gadelha
 * 
 * @param <T> : type of value (String or Number)
 */
public class BatchLayer1Output<T> implements Serializable {

	private static final long serialVersionUID = 1595860019246486013L;
	
	private String key;
	private T value;

	/**
	 * Default constructor
	 * @param key
	 * @param value
	 */
	public BatchLayer1Output(String key, T value) {
		this.key = key;
		this.value = value;
	}
	
//	public BatchLayerOutputStrToInt(String key, Long countAllEdits) {
//		this(key, countAllEdits);
//	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public T getValue() {
		return value;
	}

	public void setValue(T value) {
		this.value = value;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return key + '\t' + value.toString();
	}

}
