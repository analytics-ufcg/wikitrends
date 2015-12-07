package br.edu.ufcg.analytics.wikitrends.storage;

public class CassandraManager {

	private String migration_config_filepath = "/src/main/resources/production/migration.properties";
	
	public void setMigrationConfigFilePath(String filePath) {
		this.migration_config_filepath = filePath;
	}
	
	public String getMigrationConfigFilePath() {
		return this.migration_config_filepath;
	}

}
