package br.edu.ufcg.analytics.wikitrends;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import br.edu.ufcg.analytics.wikitrends.thrift.WikiMediaChange;

/**
 * Entry point for both batch and speed layer Apache Spark jobs.
 * 
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 */
public class Main {

	/**
	 * Usage: spark-submit --class br.edu.ufcg.analytics.wikitrends.Main JAR_FILE_NAME.jar &lt;seed&gt;
	 * @param args {@link Integer} seed
	 * @throws NumberFormatException when a non-integer seed is provided.
	 */
	public static void main(String[] args) {
		
		WikiMediaChange change = null;
		if(change == null){
			System.out.println("NULL");
		}
		
		SparkConf sparkConf = new SparkConf().setAppName("wikitrends-batch");
		
		try(JavaSparkContext jsc = new JavaSparkContext(sparkConf);){
			System.out.println("Running " + args[0] + " job!");
			int slices = 100;
			int n = 100000 * slices;
			List<Integer> l = new ArrayList<Integer>(n);
			for (int i = 0; i < n; i++) {
				l.add(i);
			}

			JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

			int count = dataSet.map(new Function<Integer, Integer>() {
				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public Integer call(Integer integer) {
					double x = Math.random() * 2 - 1;
					double y = Math.random() * 2 - 1;
					return (x * x + y * y < 1) ? 1 : 0;
				}
			}).reduce(new Function2<Integer, Integer, Integer>() {
				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public Integer call(Integer integer, Integer integer2) {
					return integer + integer2;
				}
			});

			System.out.println("Pi is roughly " + 4.0 * count / n);

			jsc.stop();
		}


	}
}

