package br.edu.ufcg.analytics.wikitrends.view.api.controller;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import br.edu.ufcg.analytics.wikitrends.view.api.controller.beans.RankingRow;

/**
 * @author Ricardo Araújo Santos - ricardo@copin.ufcg.edu.br
 *
 */
@RestController
public class LocalFileWikiTrendsController implements WikiTrendsController {

	/* (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.api.controller.WikiController#statistics()
	 */
	@Override
	@RequestMapping("/statistics")
	public RankingRow[] statistics() {
		String source = "/var/www/wikitrends/data/absolute.tsv";
		
		return query(source, Integer.MAX_VALUE);
	}

	/* (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.api.controller.WikiController#editors(java.lang.String)
	 */
	@Override
	@RequestMapping("/editors")
	public RankingRow[] editors(@RequestParam(value="size", defaultValue="20") String size) {
		String source = "/var/www/wikitrends/data/editors.tsv";
		int numberOfResults = Integer.valueOf(size);
		
		return query(source, numberOfResults);
	}

	/* (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.api.controller.WikiController#idioms(java.lang.String)
	 */
	@Override
	@RequestMapping("/idioms")
	public RankingRow[] idioms(@RequestParam(value="size", defaultValue="20") String size) {
		String source = "/var/www/wikitrends/data/idioms.tsv";
		int numberOfResults = Integer.valueOf(size);
		
		return query(source, numberOfResults);
	}

	/* (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.api.controller.WikiController#pages(java.lang.String)
	 */
	@Override
	@RequestMapping("/pages")
	public RankingRow[] pages(@RequestParam(value="size", defaultValue="20") String size, @RequestParam(value="contentonly", defaultValue="false") String contentOnly) {
		int numberOfResults = Integer.valueOf(size);
		boolean contentOnlyPages = Boolean.valueOf(contentOnly);
		String source = "/var/www/wikitrends/data/pages" + (contentOnlyPages? "_content": "") + ".tsv";
		
		return query(source, numberOfResults);
	}

	private RankingRow[] query(String source, int numberOfResults) {
		List<RankingRow> results = new ArrayList<>();
		
		try(Scanner input = new Scanner(new File(source), "utf-8");){
			while(input.hasNextLine() && results.size() < numberOfResults){
				String[] line = input.nextLine().split("\\t");
				results.add(new RankingRow(line[0], line[1]));
			}
		} catch (FileNotFoundException e) {
			System.err.println(source);
			e.printStackTrace();
		}
		return results.toArray(new RankingRow[results.size()]);
	}

}


