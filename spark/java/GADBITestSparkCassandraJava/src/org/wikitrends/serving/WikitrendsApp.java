package org.wikitrends.serving;

import org.apache.spark.api.java.JavaSparkContext;

public abstract class WikitrendsApp {
	protected JavaSparkContext sc;
	protected String className;
	
	public WikitrendsApp(JavaSparkContext sc2, String className) {
		 this.sc = sc2;
		 this.className = className;
	}
	
	public abstract void run();
	public abstract void compute();
	public abstract void showResults();

	public void setClassName(String s) {
		this.className = s;
	}
}
