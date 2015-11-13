package br.edu.ufcg.analytics.wikitrends.storage.raw;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class MigrateMasterDataset {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
        conf.setAppName("Java API demo");
        conf.setMaster("local");
        conf.set("spark.cassandra.connection.host", "localhost");
        
        JavaSparkContext sc = new JavaSparkContext(conf);
		
		TablesGenerator tGen = new TablesGenerator(sc);
		tGen.generate();
		
		DataGenerator gen = new DataGenerator(sc);
		gen.populateDatabase("/home/guilhermemg/Desktop/tailnewdata.json");
	}
}
