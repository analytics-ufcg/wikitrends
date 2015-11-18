package br.edu.ufcg.analytics.wikitrends.ingestion;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.commons.configuration.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class CustomKafkaProducer {
	
	private KafkaProducer<String,String> producer;
	private transient Configuration configuration;

	public CustomKafkaProducer(Configuration configuration) {
		this.producer = null;
		this.configuration = configuration;
	}
	
	public void initializeKafkaProducer() throws InterruptedException, ExecutionException {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.format("%s:%s", 
				configuration.getString("wikitrends.ingestion.kafka.host"), configuration.getString("wikitrends.ingestion.kafka.port")));
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

		producer = new KafkaProducer<String,String>(props);		
	}
	
	public void sendMessage(String message) throws InterruptedException, ExecutionException {
		String topic="new_edits";
		String key = "keyTest";
		String value = message;
		ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>(topic, key, value);
		producer.send(producerRecord);	
	}
}