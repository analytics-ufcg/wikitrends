package org.wikitrends.serving;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.wikitrends.serving.datagenerator.TablesGenerator;

public class Main {
	public static void main(String[] args) {
        /**if (args.length != 2) {
            System.err.println("Syntax: com.datastax.spark.demo.JavaDemo <Spark Master URL> <Cassandra contact point>");
            System.exit(1);
        }
        */

        SparkConf conf = new SparkConf();
        conf.setAppName("Java API demo");
        conf.setMaster("local");
        conf.set("spark.cassandra.connection.host", "localhost");
        
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        TablesGenerator tGen = new TablesGenerator(sc);
        tGen.generateTables();
        
        WikitrendsApp app = new TopClasses(sc, "");
        List<String> list = new ArrayList<String>();
        list.add("top_editors");
        list.add("top_content_pages");
        list.add("top_pages");
        list.add("top_idioms");
        
        for(String s : list) {
        	app.setClassName(s);
        	app.run();
        }
        
        WikitrendsApp app2 = new AbsoluteValues(sc, "absolute_values");
        app2.run();
        
        sc.stop();
    }
}
