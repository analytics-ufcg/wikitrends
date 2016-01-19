package br.edu.ufcg.analytics.wikitrends.view.api.controller;

import java.util.ArrayList;
import java.util.List;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import br.edu.ufcg.analytics.wikitrends.view.api.controller.beans.RankingRow;

/**
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 * @author Guilherme Gadelha
 * 
 */
@RestController
public class CassandraDBWikiTrendsController implements WikiTrendsController {
	
//	private JavaSparkContext sc;
	private String seedNodes;
	private Cluster cluster;
	
	public CassandraDBWikiTrendsController() {
		seedNodes = System.getProperty("spark.cassandra.connection.host", "localhost");
		cluster = Cluster.builder().addContactPoints(seedNodes.split(",")).build();
	}

	/* (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.api.controller.WikiController#statistics()
	 */
	@Override
	@RequestMapping("/v2/statistics")
	public RankingRow[] statistics() {
		String tableName = "batch_views2.absolute_values";
		
		return getValues(tableName);
	}

	/* (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.api.controller.WikiController#idioms(java.lang.String)
	 */
	@Override
	@RequestMapping("/v2/idioms")
	public RankingRow[] idioms(@RequestParam(value="size", defaultValue="20") String size) {
		int numberOfResults = Integer.valueOf(size);
		String tableName = "batch_views2.top_idioms";
		
		return getRanking(numberOfResults, tableName);
	}

	private RankingRow[] getRanking(int numberOfResults, String tableName) {
		RankingRow[] results;
		try (Session session = cluster.newSession();) {
			ResultSet resultSet = session.execute("SELECT * FROM " + tableName + " LIMIT ?", numberOfResults);
			List<Row> all = resultSet.all();
			results = new RankingRow[all.size()];

			for (int i = 0; i < results.length; i++) {
				Row row = all.get(i);
				results[i] = new RankingRow(row.getString("name"), Long.toString(row.getLong("count")));
			}
		}
		return results;
	}
	
	private RankingRow[] getValues(String tableName) {
		ArrayList<RankingRow> results = new ArrayList<RankingRow>();
		try (Session session = cluster.newSession();) {
			ResultSet resultSet = session.execute("SELECT * FROM " + tableName);
			List<Row> all = resultSet.all();

			Row row = all.get(0);
			
			for (Definition column : resultSet.getColumnDefinitions()) {
				if (column.getType().equals(DataType.bigint())){
					results.add(new RankingRow(column.getName(), Long.toString(row.getLong(column.getName()))));
				}
				else if (column.getType().equals(DataType.cint())) {
					results.add(new RankingRow(column.getName(), Integer.toString(row.getInt(column.getName()))));	
				}
			}			
		}
		return results.toArray(new RankingRow[results.size()]);
	}

	@Override
	@RequestMapping("/v2/editors")
	public RankingRow[] editors(@RequestParam(value="size", defaultValue="20") String size) {
		int numberOfResults = Integer.valueOf(size);
		String tableName = "batch_views2.top_editors";
		
		return getRanking(numberOfResults, tableName);
	}

	@Override
	@RequestMapping("/v2/pages")
	public RankingRow[] pages(@RequestParam(value="size", defaultValue="20") String size, @RequestParam(value="contentonly", defaultValue="false") String contentOnly) {
		int numberOfResults = Integer.valueOf(size);
		boolean content = Boolean.valueOf(contentOnly);
		String tableName = "batch_views2.top_" + (content?"content_":"") + "pages";
		
		return getRanking(numberOfResults, tableName);
	}
}


