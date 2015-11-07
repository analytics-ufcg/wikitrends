package br.edu.ufcg.analytics.wikitrends.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import br.edu.ufcg.analytics.wikitrends.LambdaLayer;

/**
 * {@link SparkJob} implementation when a {@link LambdaLayer#BATCH} is chosen. 
 * 
 * @author Ricardo Ara&eacute;jo Santos - ricoaraujosantos@gmail.com
 */
public class BatchLayerJob implements SparkJob {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -1348604327884764150L;
	private SparkConf conf;

	/**
	 * Default constructor
	 */
	public BatchLayerJob() {
		conf = new SparkConf();
		conf.setAppName("wikitrends-batch");
	}

	/*@ (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.spark.SparkJob#run()
	 */
	@Override
	public void run() {
		
		try(JavaSparkContext sc = new JavaSparkContext(conf);){
			System.out.println("Running batch job!");
			int slices = 100;
			int n = 100000 * slices;
			List<Integer> l = new ArrayList<Integer>(n);
			for (int i = 0; i < n; i++) {
				l.add(i);
			}

			JavaRDD<Integer> dataSet = sc.parallelize(l, slices);

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

			sc.stop();
		}	}

}
