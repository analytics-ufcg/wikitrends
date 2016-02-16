package br.edu.ufcg.analytics.wikitrends.view.api.controller;

import java.util.ArrayList;
import java.util.List;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import br.edu.ufcg.analytics.wikitrends.processing.batch.BatchViewID;
import br.edu.ufcg.analytics.wikitrends.view.api.controller.beans.RankingRow;

/**
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 * @author Guilherme Gadelha
 * 
 */
@RestController
public class CassandraDBWikiTrendsController implements WikiTrendsController {
	

	private static final String BATCH_VIEWS_KEYSPACE = "batch_views";

	private static final String FINAL_METRICS_TABLE = BATCH_VIEWS_KEYSPACE
			+ "." + BatchViewID.FINAL_METRICS.toString();

	private static final String FINAL_RANKINGS_TABLE = BATCH_VIEWS_KEYSPACE
			+ "." + "final_rankings";

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
		String tableName = FINAL_METRICS_TABLE;
		
		return getValues(tableName);
	}

	/* (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.api.controller.WikiController#idioms(java.lang.String)
	 */
	@Override
	@RequestMapping("/v2/idioms")
	public RankingRow[] idioms(@RequestParam(value="size", defaultValue="20") String size) {
		int numberOfResults = Integer.valueOf(size);
		String viewID = BatchViewID.IDIOMS_FINAL_RANKING.toString();
		
		return getRanking(numberOfResults, viewID);
	}

	private RankingRow[] getRanking(int numberOfResults, String viewID) {
		RankingRow[] results;
		try (Session session = cluster.newSession();) {
			ResultSet resultSet = session.execute("SELECT * FROM " + FINAL_RANKINGS_TABLE + " where id = ? LIMIT ?", viewID, numberOfResults);
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
			List<Row> all = session.execute("SELECT * FROM " + tableName).all();
			for (Row row : all) {
				results.add(new RankingRow(row.getString("name"), Long.toString(row.getLong("value"))));
			}
		}
		return results.toArray(new RankingRow[results.size()]);
	}

	@Override
	@RequestMapping("/v2/editors")
	public RankingRow[] editors(@RequestParam(value="size", defaultValue="20") String size) {
		int numberOfResults = Integer.valueOf(size);
		String viewID = BatchViewID.EDITORS_FINAL_RANKING.toString();
		
		return getRanking(numberOfResults, viewID);
	}

	@Override
	@RequestMapping("/v2/pages")
	public RankingRow[] pages(@RequestParam(value="size", defaultValue="20") String size, @RequestParam(value="contentonly", defaultValue="false") String contentOnly) {
		int numberOfResults = Integer.valueOf(size);
		boolean content = Boolean.valueOf(contentOnly);
		String viewID = content? BatchViewID.CONTENT_PAGES_FINAL_RANKING.toString(): BatchViewID.PAGES_FINAL_RANKING.toString();
		
		return getRanking(numberOfResults, viewID);
	}
}




