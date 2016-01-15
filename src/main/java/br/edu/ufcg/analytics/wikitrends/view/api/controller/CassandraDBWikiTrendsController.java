package br.edu.ufcg.analytics.wikitrends.view.api.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;

import br.edu.ufcg.analytics.wikitrends.view.api.controller.beans.RankingRow;

/**
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 * @author Guilherme Gadelha
 * 
 */
@RestController
public class CassandraDBWikiTrendsController implements WikiTrendsController {
	
//	private JavaSparkContext sc;
	private String seedNode;
	private Cluster cluster;
	
	public CassandraDBWikiTrendsController() {
		seedNode = "localhost";
		cluster = Cluster.builder().addContactPoints(seedNode).build();
	}

////	public CassandraDBWikiTrendsController(JavaSparkContext sc) {
////		super(); 
////		this.sc = sc;
////	}
//	
//	
//	/* (non-Javadoc)
//	 * @see br.edu.ufcg.analytics.wikitrends.api.controller.WikiController#statistics()
//	 */
//	@Override
//	@RequestMapping("/v2/statistics")
//	public RankingRow[] statistics() {
//		return queryAbsolute(Integer.MAX_VALUE);
//	}
//
//	/* (non-Javadoc)
//	 * @see br.edu.ufcg.analytics.wikitrends.api.controller.WikiController#editors(java.lang.String)
//	 */
//	@Override
//	@RequestMapping("/v2/editors")
//	public RankingRow[] editors(@RequestParam(value="size", defaultValue="20") String size) {
//		String source = "top_editors";
//		int numberOfResults = Integer.valueOf(size);
//		
//		return query(source, numberOfResults);
//	}

	/* (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.api.controller.WikiController#idioms(java.lang.String)
	 */
	@Override
	@RequestMapping("/v2/idioms")
	public RankingRow[] idioms(@RequestParam(value="size", defaultValue="20") String size) {
		int numberOfResults = Integer.valueOf(size);
		
		RankingRow[] results;
		try (Session session = cluster.newSession();) {
			ResultSet resultSet = session.execute("SELECT * FROM batch_views2.top_idioms LIMIT ?", numberOfResults);
			List<Row> all = resultSet.all();
			results = new RankingRow[all.size()];

			for (int i = 0; i < results.length; i++) {
				Row row = all.get(i);
				results[i] = new RankingRow(row.getString("name"), Integer.toString(row.getInt("count")));
			}
		}
		
		return results;
	}

	@Override
	public RankingRow[] statistics() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RankingRow[] editors(String size) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RankingRow[] pages(String size, String contentOnly) {
		// TODO Auto-generated method stub
		return null;
	}

//	/* (non-Javadoc)
//	 * @see br.edu.ufcg.analytics.wikitrends.api.controller.WikiController#pages(java.lang.String)
//	 */
//	@Override
//	@RequestMapping("/v2/pages")
//	public RankingRow[] pages(@RequestParam(value="size", defaultValue="20") String size, @RequestParam(value="contentonly", defaultValue="false") String contentOnly) {
//		int numberOfResults = Integer.valueOf(size);
//		boolean contentOnlyPages = Boolean.valueOf(contentOnly);
//		String source = contentOnlyPages ? "top_pages": "top_content_pages";
//		
//		return query(source, numberOfResults);
//	}
//	
//	private RankingRow[] query(String source, int numberOfResults) {
//		List<RankingRow> results = new ArrayList<>();
//		Map<String, Long> mapComputed = computeTopClasses(source);
//		
//		for(Entry<String, Long> s : mapComputed.entrySet()) {
//			results.add(new RankingRow(s.getKey(), s.getValue().toString()));
//		}
//		
//		return results.toArray(new RankingRow[results.size()]);
//	}
//	
//	private RankingRow[] queryAbsolute(int numberOfResults) {
//		List<RankingRow> results = new ArrayList<>();
//		
//		Map<String, Long> mapComputed = computeEditsData();
//		for(Entry<String, Long> s : mapComputed.entrySet()) {
//			results.add(new RankingRow(s.getKey(), s.getValue().toString()));
//		}
//		
//		results.add(new RankingRow("distinct_editors", computeDistinctEditorsCount().toString()));
//		results.add(new RankingRow("distinct_pages", computeDistinctPagesCount().toString()));
//		results.add(new RankingRow("distinct_servers", computeDistinctServersCount().toString()));
//		results.add(new RankingRow("origin", computeSmallerOrigin().toString()));
//		
//		return results.toArray(new RankingRow[results.size()]);
//	}
	
}


