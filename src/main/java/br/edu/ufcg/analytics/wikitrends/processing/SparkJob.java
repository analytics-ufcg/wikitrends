package br.edu.ufcg.analytics.wikitrends.processing;

import java.io.Serializable;

/**
 * Models a job runnable on Apache Spark framework.
 * 
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 */
public interface SparkJob extends Serializable{

	/**
	 * Triggers the execution of this job on a Apache Spark cluster.
	 */
	void run();

}
