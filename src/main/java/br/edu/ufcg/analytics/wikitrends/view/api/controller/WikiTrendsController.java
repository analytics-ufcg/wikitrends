package br.edu.ufcg.analytics.wikitrends.view.api.controller;

import java.util.List;

import com.datastax.driver.core.Row;

import br.edu.ufcg.analytics.wikitrends.view.api.controller.beans.RankingRow;

public interface WikiTrendsController {

	RankingRow[] statistics();

	RankingRow[] editors(String size);

	RankingRow[] idioms(String size);

	RankingRow[] pages(String size, String contentOnly);

}