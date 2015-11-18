package br.edu.ufcg.analytics.wikitrends.storage.raw;

public class MigrateMasterDataset {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		String seedNode = args[0];
		String inputFile = args[1];
		
		new DataGenerator(seedNode, inputFile).run();
	}
}
