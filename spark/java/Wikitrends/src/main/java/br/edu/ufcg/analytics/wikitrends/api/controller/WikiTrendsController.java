package br.edu.ufcg.analytics.wikitrends.api.controller;

import br.edu.ufcg.analytics.wikitrends.api.controller.beans.RankingRow;

public interface WikiTrendsController {

	RankingRow[] statistics();

	RankingRow[] editors(String size);

	RankingRow[] idioms(String size);

	RankingRow[] pages(String size);

}