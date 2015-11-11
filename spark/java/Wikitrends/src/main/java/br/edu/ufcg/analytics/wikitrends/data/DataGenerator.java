package br.edu.ufcg.analytics.wikitrends.data;

import java.io.File;
import java.util.Scanner;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class DataGenerator {
	public void populateDatabase(String path) {
		try {
			@SuppressWarnings("resource")
			Scanner scan = new Scanner(new File(path));
		    while(scan.hasNextLine()){
		        String line = scan.nextLine();
		        
		        JsonElement obj = new JsonParser().parse(line).getAsJsonObject();

		        System.out.println(obj.toString());
				//if(obj.get("type") ) {
				//}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
