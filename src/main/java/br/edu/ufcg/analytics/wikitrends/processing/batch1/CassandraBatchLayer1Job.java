package br.edu.ufcg.analytics.wikitrends.processing.batch1;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;

import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditType;
import br.edu.ufcg.analytics.wikitrends.storage.serving1.types.AbsoluteValuesShot;
import br.edu.ufcg.analytics.wikitrends.storage.serving1.types.TopClass;

public class CassandraBatchLayer1Job extends BatchLayer1Job {

	private static final long serialVersionUID = -1876586531051844584L;

	protected String batchViewsKeyspace;
	private String pagesTable;
	private String contentPagesTable;
	private String serversTable;
	protected String serversRankingTable;
	protected String usersTable;
	private String absoluteValuesTable;

	/**
	 * Default constructor
	 * 
	 * @param configuration
	 */
	public CassandraBatchLayer1Job(Configuration configuration) {
		super(configuration);
		batchViewsKeyspace = configuration.getString("wikitrends.batch.cassandra.keyspace");
		pagesTable = configuration.getString("wikitrends.batch.cassandra.table.pages");
		contentPagesTable = configuration.getString("wikitrends.batch.cassandra.table.contentpages");
		serversTable = configuration.getString("wikitrends.batch.cassandra.table.servers");
		serversRankingTable = configuration.getString("wikitrends.batch.cassandra.table.serversranking");
		usersTable = configuration.getString("wikitrends.batch.cassandra.table.users");
		absoluteValuesTable = configuration.getString("wikitrends.batch.cassandra.table.absolutevalues");
		
	}

	/* (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.processing.batch1.BatchLayer1Job#readRDD(org.apache.spark.api.java.JavaSparkContext)
	 */
	@Override
	protected JavaRDD<EditType> readRDD(JavaSparkContext sc) {
		JavaRDD<EditType> wikipediaEdits = javaFunctions(sc).cassandraTable("master_dataset", "edits")
				.select("event_time", "common_event_bot", "common_event_title", "common_server_name", "common_event_user",
						"common_event_namespace", "edit_minor", "edit_length")
				.map(new Function<CassandraRow, EditType>() {
					private static final long serialVersionUID = 1L;

					@Override
					public EditType call(CassandraRow v1) throws Exception {
						EditType edit = new EditType();
						edit.setEvent_time(v1.getDate("event_time"));
						edit.setCommon_event_bot(v1.getBoolean("common_event_bot"));
						edit.setCommon_event_title(v1.getString("common_event_title"));
						edit.setCommon_event_user(v1.getString("common_event_user"));
						edit.setCommon_event_namespace(v1.getInt("common_event_namespace"));
						edit.setCommon_server_name(v1.getString("common_server_name"));
						edit.setEditMinor(v1.getBoolean("edit_minor"));
						edit.setEdit_length(v1.getMap("edit_length", CassandraJavaUtil.typeConverter(String.class), CassandraJavaUtil.typeConverter(Long.class)));
						return edit;
					}

				});
		return wikipediaEdits;
	}

	@Override
	protected void saveTitleRanking(JavaSparkContext sc, JavaRDD<BatchLayer1Output<Integer>> titleRanking) {
		List<BatchLayer1Output<Integer>> allPages = titleRanking.take(100); // data map
		
		Map<String, Integer> data = new HashMap<String, Integer>();
		for(BatchLayer1Output<Integer> t  : allPages) {
			data.put(t.getKey(), t.getValue());
		}
		
		List<TopClass> output = Arrays.asList(new TopClass(data));
		
		CassandraJavaUtil.javaFunctions(sc.parallelize(output))
			.writerBuilder(batchViewsKeyspace, pagesTable, mapToRow(TopClass.class))
			.saveToCassandra();
		
	}

	@Override
	protected void saveContentTitleRanking(JavaSparkContext sc, JavaRDD<BatchLayer1Output<Integer>> contentTitleRanking) {
		List<BatchLayer1Output<Integer>> allPages = contentTitleRanking.take(100); // data map
		
		Map<String, Integer> data = new HashMap<String, Integer>();
		for(BatchLayer1Output<Integer> t  : allPages) {
			data.put(t.getKey(), t.getValue());
		}
		
		List<TopClass> output = Arrays.asList(new TopClass(data));
		
		CassandraJavaUtil.javaFunctions(sc.parallelize(output))
			.writerBuilder(batchViewsKeyspace, contentPagesTable, mapToRow(TopClass.class))
			.saveToCassandra();
		
	}

	/* (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.processing.batch1.BatchLayer1Job#saveServerRanking(org.apache.spark.api.java.JavaSparkContext, org.apache.spark.api.java.JavaRDD)
	 */
	@Override
	protected void saveServerRanking(JavaSparkContext sc, JavaRDD<BatchLayer1Output<Integer>> serverRanking) {
		
		List<BatchLayer1Output<Integer>> allPages = serverRanking.collect();
		
		Map<String, Integer> data = new HashMap<String, Integer>();
		for(BatchLayer1Output<Integer> t  : allPages) {
			data.put(t.getKey(), t.getValue());
		}
		
		List<TopClass> output = Arrays.asList(new TopClass(data));
		
		CassandraJavaUtil.javaFunctions(sc.parallelize(output))
			.writerBuilder(batchViewsKeyspace, serversTable, mapToRow(TopClass.class))
			.saveToCassandra();
		
	}

	/* (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.processing.batch1.BatchLayer1Job#saveUserRanking(org.apache.spark.api.java.JavaSparkContext, org.apache.spark.api.java.JavaRDD)
	 */
	@Override
	protected void saveUserRanking(JavaSparkContext sc, JavaRDD<BatchLayer1Output<Integer>> userRanking) {
		List<BatchLayer1Output<Integer>> allPages = userRanking.take(100);
		
		Map<String, Integer> data = new HashMap<String, Integer>();
		for(BatchLayer1Output<Integer> t  : allPages) {
			data.put(t.getKey(), t.getValue());
		}
		
		List<TopClass> output = Arrays.asList(new TopClass(data));
		
		CassandraJavaUtil.javaFunctions(sc.parallelize(output))
			.writerBuilder(batchViewsKeyspace, usersTable, mapToRow(TopClass.class))
			.saveToCassandra();
	}

	/* (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.processing.batch1.BatchLayer1Job#processStatistics(org.apache.spark.api.java.JavaSparkContext, org.apache.spark.api.java.JavaRDD)
	 */
	@Override
	protected void processStatistics(JavaSparkContext sc, JavaRDD<EditType> wikipediaEdits) {
		Map<String, Long> edits_data = new HashMap<String, Long>();
		edits_data.put("all_edits", countAllEdits(wikipediaEdits));
		edits_data.put("minor_edits", countMinorEdits(wikipediaEdits));
		edits_data.put("average_size", calcAverageEditLength(wikipediaEdits));

		
//		Set<String> distincts_pages_set = new HashSet<>();
//		Set<String> distincts_editors_set = new HashSet<>();
//		Set<String> distincts_servers_set = new HashSet<>();

		Set<String> distincts_pages_set = distinctPages(wikipediaEdits);
		Set<String> distincts_editors_set = distinctEditors(wikipediaEdits);
		Set<String> distincts_servers_set = distinctServers(wikipediaEdits);
		
//		System.out.println(distincts_pages_set.size()); // 359185
//		System.out.println(distincts_editors_set.size()); // 57978
//		System.out.println(distincts_servers_set.size()); // 215

		Long smaller_origin = getOrigin(wikipediaEdits); 
		
		List<AbsoluteValuesShot> output = Arrays.asList(new AbsoluteValuesShot(edits_data, 
																				distincts_pages_set,
																				distincts_editors_set,
																				distincts_servers_set,
																				smaller_origin));
		CassandraJavaUtil.javaFunctions(sc.parallelize(output))
			.writerBuilder(batchViewsKeyspace, absoluteValuesTable, mapToRow(AbsoluteValuesShot.class))
			.saveToCassandra();
	}
}
