package br.edu.ufcg.analytics.wikitrends.processing.batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import br.edu.ufcg.analytics.wikitrends.storage.raw.types.EditType;

/**
 * @author Ricardo Araújo Santos - ricoaraujosantos@gmail.com
 *
 */
public class HDFSBatchLayerJob extends BatchLayerJob {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4753586703404568254L;
	private static BatchLayerOutput PAGES_HEADER = new BatchLayerOutput("title", "count");
	private static BatchLayerOutput SERVERS_HEADER = new BatchLayerOutput("server", "count");
	private static BatchLayerOutput USERS_HEADER = new BatchLayerOutput("user", "count");
	private static BatchLayerOutput ABSOLUTE_HEADER = new BatchLayerOutput("field", "count");

	private String inputFile;
	private String pagesFile;
	private String contentPagesFile;
	private String serversFile;
	private String usersFile;
	private String absoluteValuesFile;
	

	public HDFSBatchLayerJob(Configuration configuration) {
		super(configuration);
		
		inputFile = configuration.getString("wikitrends.batch.hdfs.input");
		
		String outputPath = String.format("hdfs://%s:%s/%s/", 
				configuration.getString("wikitrends.batch.hdfs.hostname"),
				configuration.getString("wikitrends.batch.hdfs.port"),
				configuration.getString("wikitrends.batch.hdfs.output"));
		
		pagesFile = outputPath + configuration.getString("wikitrends.batch.hdfs.output.pages");
		contentPagesFile = outputPath + configuration.getString("wikitrends.batch.hdfs.output.contentpages");
		serversFile = outputPath + configuration.getString("wikitrends.batch.hdfs.output.servers");
		usersFile = outputPath + configuration.getString("wikitrends.batch.hdfs.output.users");
		absoluteValuesFile = outputPath + configuration.getString("wikitrends.batch.hdfs.output.absolute");
		
	}

	@Override
	protected JavaRDD<EditType> readRDD(JavaSparkContext sc) {
		JavaRDD<EditType> wikipediaEdits = sc.textFile(inputFile).map(l -> new JsonParser().parse(l).getAsJsonObject())
				.filter(edit -> {
			          String type = edit.get("type").getAsString();
			          return "new".equals(type) || "edit".equals(type);
				}).map(jsonObject -> {
					EditType edit = new EditType();
					edit.setCommon_event_type(jsonObject.get("type").getAsString());
					edit.setCommon_event_bot(jsonObject.get("bot").getAsBoolean());
					edit.setCommon_event_title(jsonObject.get("title").getAsString());
					edit.setCommon_event_user(jsonObject.get("user").getAsString());
					edit.setCommon_event_namespace(jsonObject.get("namespace").getAsString());
					edit.setCommon_server_name(jsonObject.get("server_name").getAsString());
					edit.setEdit_minor(jsonObject.get("minor").getAsBoolean());
					Map<String, Integer> length = new HashMap<>();
					JsonObject lengthObject = jsonObject.get("length").getAsJsonObject();
					if(!lengthObject.get("old").isJsonNull()){
						length.put("old", lengthObject.get("old").getAsInt());
					}
					if(!lengthObject.get("new").isJsonNull()){
						length.put("new", lengthObject.get("new").getAsInt());
					}
					edit.setEdit_length(length);
					return edit;
				});
		return wikipediaEdits;
	}

	@Override
	protected void saveTitleRanking(JavaSparkContext sc, JavaRDD<BatchLayerOutput> titleRanking) {
		List<BatchLayerOutput> allPages = titleRanking.take(100);
		allPages.add(0, PAGES_HEADER);
		
		for (BatchLayerOutput batchLayerOutput : allPages) {
			System.out.println(batchLayerOutput);
		}
		
		sc.parallelize(allPages).coalesce(1).saveAsTextFile(pagesFile);
	}

	@Override
	protected void saveContentTitleRanking(JavaSparkContext sc, JavaRDD<BatchLayerOutput> contentTitleRanking) {
		List<BatchLayerOutput> allPages = contentTitleRanking.take(100);
		allPages.add(0, PAGES_HEADER);
		
		sc.parallelize(allPages).coalesce(1).saveAsTextFile(contentPagesFile);
	}

	@Override
	protected void saveServerRanking(JavaSparkContext sc, JavaRDD<BatchLayerOutput> serverRanking) {
		List<BatchLayerOutput> allPages = serverRanking.collect();
		allPages.add(0, SERVERS_HEADER);
		
		sc.parallelize(allPages).coalesce(1).saveAsTextFile(serversFile);
	}

	@Override
	protected void saveUserRanking(JavaSparkContext sc, JavaRDD<BatchLayerOutput> userRanking) {
		List<BatchLayerOutput> allPages = userRanking.take(100);
		allPages.add(0, USERS_HEADER);
		
		sc.parallelize(allPages).coalesce(1).saveAsTextFile(usersFile);
	}

	@Override
	protected void processStatistics(JavaSparkContext sc, JavaRDD<EditType> wikipediaEdits) {

		List<BatchLayerOutput> statistics = new ArrayList<>();
		statistics.add(ABSOLUTE_HEADER);
		statistics.add(new BatchLayerOutput("all_edits", countAllEdits(wikipediaEdits)));
		statistics.add(new BatchLayerOutput("minor_edits", countMinorEdits(wikipediaEdits)));
		statistics.add(new BatchLayerOutput("average_size", calcAverageEditLength(wikipediaEdits)));
		statistics.add(new BatchLayerOutput("distinct_pages", distinctPages(wikipediaEdits)));
		statistics.add(new BatchLayerOutput("distinct_editors", distinctEditors(wikipediaEdits)));
		statistics.add(new BatchLayerOutput("distinct_servers", distinctServers(wikipediaEdits)));
		statistics.add(new BatchLayerOutput("origin", getOrigin(wikipediaEdits)));

		sc.parallelize(statistics).coalesce(1).saveAsTextFile(absoluteValuesFile);
	}

}
