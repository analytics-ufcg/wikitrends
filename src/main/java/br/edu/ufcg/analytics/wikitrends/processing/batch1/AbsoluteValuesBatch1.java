package br.edu.ufcg.analytics.wikitrends.processing.batch1;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditType;
import br.edu.ufcg.analytics.wikitrends.storage.serving1.types.AbsoluteValuesShot;

public class AbsoluteValuesBatch1 extends BatchLayer1Job {

	private static final long serialVersionUID = 4394268380743075556L;

	private String absoluteValuesTable;

	public AbsoluteValuesBatch1(Configuration configuration, JavaSparkContext jsc) {
		super(configuration, jsc);
		
		absoluteValuesTable = configuration.getString("wikitrends.batch.cassandra.table.absolutevalues");
	}

	public void process() {
		JavaRDD<EditType> wikipediaEdits = read()
				.filter(edit -> edit.getCommon_server_name().endsWith("wikipedia.org"))
				.cache();
		
		Map<String, Long> edits_data = new HashMap<String, Long>();
		edits_data.put("all_edits", countAllEdits(wikipediaEdits));
		edits_data.put("minor_edits", countMinorEdits(wikipediaEdits));
		edits_data.put("average_size", calcAverageEditLength(wikipediaEdits));

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
		
		CassandraJavaUtil.javaFunctions(getJavaSparkContext().parallelize(output))
			.writerBuilder(getBatchViewsKeyspace(), absoluteValuesTable, mapToRow(AbsoluteValuesShot.class))
			.saveToCassandra();
	}
	

	private Long countAllEdits(JavaRDD<EditType> wikipediaEdits) {
		return wikipediaEdits.count();
	}

	private Long countMinorEdits(JavaRDD<EditType> wikipediaEdits) {
		return wikipediaEdits.filter(edit -> {
			return edit.getEditMinor() != null && edit.getEditMinor();
		}).count();
	}

	private Long calcAverageEditLength(JavaRDD<EditType> wikipediaEdits) {
		JavaRDD<Long> result = wikipediaEdits.map( edit -> {
			Map<String, Long> length = edit.getEdit_length();
			long oldLength = length.containsKey("old")? length.get("old"): 0;
			long newLength = length.containsKey("new")? length.get("new"): 0;
			return newLength - oldLength;
		});
		return result.reduce((a, b) -> a+b) / result.count();
	}

	private Set<String> distinctPages(JavaRDD<EditType> wikipediaEdits) {
		return new HashSet<String>(wikipediaEdits.map(edit -> edit.getCommon_event_title()).distinct().collect());
	}

	private Set<String> distinctServers(JavaRDD<EditType> wikipediaEdits) {
		return new HashSet<String>(wikipediaEdits.map(edit -> edit.getCommon_server_name()).distinct().collect());
	}

	private Set<String> distinctEditors(JavaRDD<EditType> wikipediaEdits) {
		return new HashSet<String>(
				wikipediaEdits.filter(edit -> {
					return edit.getCommon_event_bot() != null && !edit.getCommon_event_bot();
				})
				.map(edit -> edit.getCommon_event_user()).distinct().collect());
	}

	private Long getOrigin(JavaRDD<EditType> wikipediaEdits) {
		return wikipediaEdits.first().getEvent_time().getTime();
	}

}
