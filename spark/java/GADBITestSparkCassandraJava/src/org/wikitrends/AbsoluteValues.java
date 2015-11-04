
package org.wikitrends;

import static com.datastax.spark.connector.CassandraJavaUtil.javaFunctions;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;

public class AbsoluteValues implements Serializable {
    private transient SparkConf conf;

    private AbsoluteValues(SparkConf conf) {
        this.conf = conf;
    }

    private void run() {
        JavaSparkContext sc = new JavaSparkContext(conf);
        generateData(sc);
        //compute(sc);
        //showResults(sc);
        loadData(sc);
        sc.stop();
    }

    private void generateData(JavaSparkContext sc) {
        CassandraConnector connector = CassandraConnector.apply(sc.getConf());

        // Prepare the schema
        try (Session session = connector.openSession()) {
            session.execute("DROP KEYSPACE IF EXISTS batch_views");
            session.execute("CREATE KEYSPACE batch_views WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("CREATE TABLE IF NOT EXISTS batch_views.absolute_values (" +
            				    "id INT," +
            				    "date TEXT," +
            				    "hour TEXT," +
            					"all_edits INT," +
            					"minor_edits INT," +
            					"average_size INT," +
            					"distinct_pages INT," +
            					"distinct_servers INT," +
            					"distinct_editors INT," +
            					"origin BIGINT," +
            					"batch_elapsed_time BIGINT," +
            					"total_executor_cores INT," +
            					"input_size BIGINT," +
            					"event_time TIMESTAMP," +
            					
								"PRIMARY KEY((id, date), event_time)," +
								") WITH CLUSTERING ORDER BY (event_time DESC);"
                );
        }
        
        Integer all_edits =	3758062;
        Integer minor_edits =	600606;
        Integer average_size = 209;
        Integer distinct_pages = 1550572;
        Integer distinct_editors = 471502;
        Integer distinct_servers = 280;
        Long origin = 1444077595L; // in milliseconds (?)
        Long batch_e_time = 1392315L; // in milliseconds (?)
        Integer total_executor_cores = 4;
        Long input_size = 5145694870L; // in bytes (?)
        
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
        SimpleDateFormat hourFormatter = new SimpleDateFormat("HH", Locale.ENGLISH);
        
        List<AbsoluteValuesShot> listAbsoluteValues = new ArrayList<AbsoluteValuesShot>();
        for(int i = 0; i < 6; i++) {
        	Date event_time = new Date();
        	listAbsoluteValues.add(new AbsoluteValuesShot(
        									i,
        									dateFormatter.format(event_time),
        									hourFormatter.format(event_time),
        									all_edits+i*10000000,
        									minor_edits+i*1000000,
        									average_size+i,
        									distinct_pages+i,
        									distinct_editors+i,
        									distinct_servers+i,
        									origin+(i*100000),
        									batch_e_time+(i*1000L),
        									total_executor_cores,
        									input_size+i*10,
        									event_time)
        			);
        	try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }

        JavaRDD<AbsoluteValuesShot> topEditorsRDD = sc.parallelize(listAbsoluteValues);
        Integer num = (int) topEditorsRDD.count();
        System.out.println("#Objects: " + num);
        
        JavaRDD<AbsoluteValuesShot> topEditorsRDD2 = topEditorsRDD.filter(
        		new Function<AbsoluteValuesShot, Boolean>() {

					@Override
					public Boolean call(AbsoluteValuesShot te) throws Exception {
						return te.batch_elapsed_time > 0;
					}
        			
        		}
        );
        Integer num2 = (int) topEditorsRDD2.count();
        System.out.println("#AbsoluteValuesShot.batch_elapsed_time > 0: " + num2);
        System.out.println("filter first element: " + topEditorsRDD2.first());
               
        javaFunctions(topEditorsRDD2, AbsoluteValuesShot.class).saveToCassandra("batch_views", "absolute_values");

    }

    /*private void compute(JavaSparkContext sc) {
        JavaPairRDD<Integer, TopEditor> topEditorsRDD = javaFunctions(sc)
                .cassandraTable("top_editors_batch_view", "top_editors", TopEditor.class)
                .keyBy(new Function<TopEditor, Integer>() {
                    @Override
                    public Integer call(TopEditor tEditor) throws Exception {
                        return tEditor.getID();
                    }
                });

        JavaPairRDD<Integer, BigDecimal> allSalesRDD = joinedRDD.flatMap(new PairFlatMapFunction<Tuple2<Integer, Tuple2<Sale, Product>>, Integer, BigDecimal>() {
            @Override
            public Iterable<Tuple2<Integer, BigDecimal>> call(Tuple2<Integer, Tuple2<Sale, Product>> input) throws Exception {
                Tuple2<Sale, Product> saleWithProduct = input._2();
                List<Tuple2<Integer, BigDecimal>> allSales = new ArrayList<>(saleWithProduct._2().getParents().size() + 1);
                allSales.add(new Tuple2<>(saleWithProduct._1().getProduct(), saleWithProduct._1().getPrice()));
                for (Integer parentProduct : saleWithProduct._2().getParents()) {
                    allSales.add(new Tuple2<>(parentProduct, saleWithProduct._1().getPrice()));
                }
                return allSales;
            }
        });

        JavaRDD<Summary> summariesRDD = allSalesRDD.reduceByKey(new Function2<BigDecimal, BigDecimal, BigDecimal>() {
            @Override
            public BigDecimal call(BigDecimal v1, BigDecimal v2) throws Exception {
                return v1.add(v2);
            }
        }).map(new Function<Tuple2<Integer, BigDecimal>, Summary>() {
            @Override
            public Summary call(Tuple2<Integer, BigDecimal> input) throws Exception {
                return new Summary(input._1(), input._2());
            }
        });

        javaFunctions(summariesRDD, Summary.class).saveToCassandra("java_api", "summaries");
    }

    private void showResults(JavaSparkContext sc) {
        JavaPairRDD<Integer, Summary> summariesRdd = javaFunctions(sc)
                .cassandraTable("java_api", "summaries", Summary.class)
                .keyBy(new Function<Summary, Integer>() {
                    @Override
                    public Integer call(Summary summary) throws Exception {
                        return summary.getProduct();
                    }
                });

        JavaPairRDD<Integer, Product> topEditorsRDD = javaFunctions(sc)
                .cassandraTable("java_api", "products", Product.class)
                .keyBy(new Function<Product, Integer>() {
                    @Override
                    public Integer call(Product product) throws Exception {
                        return product.getId();
                    }
                });

        List<Tuple2<Product, Optional<Summary>>> results = topEditorsRDD.leftOuterJoin(summariesRdd).values().toArray();

        for (Tuple2<Product, Optional<Summary>> result : results) {
            System.out.println(result);
        }
    }*/
    
    private void loadData(JavaSparkContext sc) {
    	CassandraConnector connector = CassandraConnector.apply(sc.getConf());
    	
    	try (Session session = connector.openSession()) {
            ResultSet results = session.execute("SELECT * FROM batch_views.absolute_values");
            
            System.out.println(String.format("id, date, event_time, all_edits, average_size, batch_elapse_time, distinct_editors, distinct_pages, distinct_servers, hour, input_size, minor_edits, origin, total_executor_cores"));
        	for (Row row : results) {
        		System.out.println(row.toString());
        	}
            System.out.println();
        }
    }

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

        AbsoluteValues app = new AbsoluteValues(conf);
        app.run();
    }
    

    public static class AbsoluteValuesShot implements Serializable {
    	private Integer id;
    	private String date;
		private Integer all_edits;
		private Integer minor_edits;
		private Integer average_size;
		private Integer distinct_pages;
		private Integer distinct_editors;
		private Integer distinct_servers;
		private Long origin;
		private Long batch_elapsed_time;
		private Integer total_executor_cores;
		private Long input_size;
		private String hour;
		private Date event_time;

        public AbsoluteValuesShot() { }

        public AbsoluteValuesShot(Integer id, String date, String hour, Integer all_edits, Integer minor_edits,
        		Integer average_size, Integer distinct_pages, 
        		Integer distinct_editors, Integer distinct_servers,
        		Long origin, Long batch_elapsed_time,
        		Integer total_executor_cores, Long input_size,
        		Date event_time) {
            this.id = id;
        	this.date = date;
        	this.hour = hour;
        	this.all_edits = all_edits;
            this.minor_edits =	minor_edits;
            this.average_size = average_size;
            this.distinct_pages = distinct_pages;
            this.distinct_editors = distinct_editors;
            this.distinct_servers = distinct_servers;
            this.origin = origin; // in milliseconds (?)
            this.batch_elapsed_time = batch_elapsed_time; // in milliseconds (?)
            this.total_executor_cores = total_executor_cores;
            this.input_size = input_size; // in bytes (?)
            this.event_time = event_time;
        }
        
        public Integer getid() {
			return id;
		}

		public void setid(Integer id) {
			this.id = id;
		}

		public String getDate() {
			return date;
		}

		public void setDate(String date) {
			this.date = date;
		}

		public Integer getAll_edits() {
			return all_edits;
		}

		public void setAll_edits(Integer all_edits) {
			this.all_edits = all_edits;
		}

		public Integer getMinor_edits() {
			return minor_edits;
		}

		public void setMinor_edits(Integer minor_edits) {
			this.minor_edits = minor_edits;
		}

		public Integer getAverage_size() {
			return average_size;
		}

		public void setAverage_size(Integer average_size) {
			this.average_size = average_size;
		}

		public Integer getDistinct_pages() {
			return distinct_pages;
		}

		public void setDistinct_pages(Integer distinct_pages) {
			this.distinct_pages = distinct_pages;
		}

		public Integer getDistinct_editors() {
			return distinct_editors;
		}

		public void setDistinct_editors(Integer distinct_editors) {
			this.distinct_editors = distinct_editors;
		}

		public Integer getDistinct_servers() {
			return distinct_servers;
		}

		public void setDistinct_servers(Integer distinct_servers) {
			this.distinct_servers = distinct_servers;
		}

		public Long getOrigin() {
			return origin;
		}

		public void setOrigin(Long origin) {
			this.origin = origin;
		}

		public Long getBatch_elapsed_time() {
			return batch_elapsed_time;
		}

		public void setBatch_elapsed_time(Long batch_elapsed_time) {
			this.batch_elapsed_time = batch_elapsed_time;
		}

		public Integer getTotal_executor_cores() {
			return total_executor_cores;
		}

		public void setTotal_executor_cores(Integer total_executor_cores) {
			this.total_executor_cores = total_executor_cores;
		}

		public Long getInput_size() {
			return input_size;
		}

		public void setInput_size(Long input_size) {
			this.input_size = input_size;
		}

    	public String getHour() {
			return hour;
		}

		public void setHour(String hour) {
			this.hour = hour;
		}

		public Date getEvent_time() {
			return event_time;
		}

		public void setEvent_time(Date event_time) {
			this.event_time = event_time;
		}

		@Override
		public String toString() {
			return "AbsoluteValuesShot [id=" + id + ", date=" + date + ", all_edits=" + all_edits + ", minor_edits="
					+ minor_edits + ", average_size=" + average_size + ", distinct_pages=" + distinct_pages
					+ ", distinct_editors=" + distinct_editors + ", distinct_servers=" + distinct_servers + ", origin="
					+ origin + ", batch_elapsed_time=" + batch_elapsed_time + ", total_executor_cores="
					+ total_executor_cores + ", input_size=" + input_size + ", hour=" + hour + ", event_time="
					+ event_time + "]";
		}
    }
}