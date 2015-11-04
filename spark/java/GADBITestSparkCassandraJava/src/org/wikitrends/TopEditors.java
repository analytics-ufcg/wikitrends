
package org.wikitrends;

import static com.datastax.spark.connector.CassandraJavaUtil.javaFunctions;

import java.io.Serializable;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
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

public class TopEditors implements Serializable {
    private transient SparkConf conf;

    private TopEditors(SparkConf conf) {
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
            session.execute("CREATE TABLE IF NOT EXISTS batch_views.top_editors (" +
            				    "id INT," +
            				    "date_event TIMESTAMP," +
            					"hour INT," +
            					"event_time TIMESTAMP," +
            					"data MAP<TEXT,INT>," +
            					
                				"PRIMARY KEY((date_event, hour), event_time)," +
                				") WITH CLUSTERING ORDER BY (event_time DESC);"
                );
        }
        
        final Map<String, Integer> m1;
        {
        	m1 = new HashMap<String, Integer>();
        	m1.put("john_1", 2);
        	m1.put("john_2", 0);
        	m1.put("john_3", 4);
        	m1.put("john_4", 3);
        	m1.put("john_5", 10);
        };
        final Map<String, Integer> m2;
        {
        	m2 = new HashMap<String, Integer>();
        	m2.put("john_1", 2);
        	m2.put("john_2", 0);
        	m2.put("john_3", 4);
        	m2.put("john_4", 3);
        	m2.put("john_5", 10);
        };
        final Map<String, Integer> m3;
        {
        	m3 = new HashMap<String, Integer>();
        	m3.put("john_1", 2);
        	m3.put("john_2", 0);
        	m3.put("john_3", 4);
        	m3.put("john_4", 3);
        	m3.put("john_5", 10);
        };
        final Map<String, Integer> m4;
        {
        	m4 = new HashMap<String, Integer>();
        	m4.put("john_1", 2);
        	m4.put("john_2", 0);
        	m4.put("john_3", 4);
        	m4.put("john_4", 3);
        	m4.put("john_5", 10);
        };
        
        Date dt11 = null, dt12 = null, dt13 = null, dt14 = null, dt21 = null, dt22 = null, dt23 = null, dt24 = null;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
        SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.ENGLISH);
        try{
	        dt11 = formatter.parse("2013-04-04");
	        dt12 = formatter.parse("2013-04-03");
	        dt13 = formatter.parse("2013-04-04");
	        dt14 = formatter.parse("2013-04-03");
	        
	        dt21 = formatter2.parse("2013-04-04 07:02");
	        dt22 = formatter2.parse("2013-04-04 07:01");
	        dt23 = formatter2.parse("2013-04-03 07:02");
	        dt24 = formatter2.parse("2013-04-03 07:01");
        }
        catch(Exception e) {
        	System.out.println(e.getMessage());
        }
        
        // Prepare the products hierarchy
        List<TopEditor> topEditors = Arrays.asList(
                new TopEditor(1, dt11, 7, dt21, m1),
                new TopEditor(2, dt12, 7, dt22, m2),
                new TopEditor(3, dt13, 7, dt23, m3),
                new TopEditor(4, dt14, 7, dt24, m4)
        );

        JavaRDD<TopEditor> topEditorsRDD = sc.parallelize(topEditors);
        Integer num = (int) topEditorsRDD.count();
        System.out.println("#Objects: " + num);
        
        JavaRDD<TopEditor> topEditorsRDD2 = topEditorsRDD.filter(
        		new Function<TopEditor, Boolean>() {

					@Override
					public Boolean call(TopEditor te) throws Exception {
						return te.hour == 7;
					}
        			
        		}
        );
        Integer num2 = (int) topEditorsRDD2.count();
        System.out.println("#TopEditors.hour == 7: " + num2);
        System.out.println("filter first element: " + topEditorsRDD2.first());
               
        javaFunctions(topEditorsRDD2, TopEditor.class).saveToCassandra("batch_views", "top_editors");

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
            ResultSet results = session.execute("SELECT * FROM top_editors_batch_view.top_editors");
            
            System.out.println(String.format("%-10s\t%-30s\t%-10s\t%-30s\t%-20s", "id", "date_event", "hour", "event_time", "data"));
        	for (Row row : results) {
        		System.out.println(String.format("%-10s\t%-30s\t%-10s\t%-30s\t%-20s", 
        													row.getInt("id"), 
        													row.getDate("date_event"),
        													row.getInt("hour"),
        													row.getDate("event_time"),
        													row.getMap("data", String.class, Integer.class).toString()));
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

        TopEditors app = new TopEditors(conf);
        app.run();
    }
    

    public static class TopEditor implements Serializable {
    	private Integer id;
    	private Date date_event;
        private Integer hour;
        private Date event_time;
        private Map<String, Integer> data;

        public TopEditor() { }

        public TopEditor(Integer id, Date date_event, Integer hour, Date event_time, Map<String, Integer> data) {
            this.id = id;
        	this.date_event = date_event;
            this.hour = hour;
            this.event_time = event_time;
            this.data = data;
        }
        
        public void setid(Integer id) {
        	this.id = id;
        }
        
        public Integer getid() {
        	return id;
        }

		public Integer getHour() {
			return hour;
		}

		public void setHour(Integer hour) {
			this.hour = hour;
		}
		
		public Date getDate_event() {
			return date_event;
		}
		
		public void setDate_event(Date date_event) {
			this.date_event = date_event;
		}

		
		public Date getEvent_time() {
			return event_time;
		}

		public void setEvent_time(Date event_time) {
			this.event_time = event_time;
		}

		public Map<String, Integer> getData() {
			return data;
		}

		public void setData(Map<String, Integer> data) {
			this.data = data;
		}
		
		@Override
        public String toString() {
            return MessageFormat.format("TopEditor: '{'ID={0},Hour={1},Data={2},Date={3},EventTime={4}'}'", id, hour, data, date_event, event_time);
        }
    }
}