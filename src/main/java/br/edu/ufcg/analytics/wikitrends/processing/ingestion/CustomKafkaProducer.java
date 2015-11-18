package br.edu.ufcg.analytics.wikitrends.processing.ingestion;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * An example using the new java client Producer for Kafka 0.8.2
 * 
 * 2015/02/27
 * @author Cameron Gregory, http://www.bloke.com/
 */
public class CustomKafkaProducer {
	
	private KafkaProducer<String,String> producer;

	public CustomKafkaProducer() {
		this.producer = null;
	}
	
	public void initializeKafkaProducer() throws InterruptedException, ExecutionException {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
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