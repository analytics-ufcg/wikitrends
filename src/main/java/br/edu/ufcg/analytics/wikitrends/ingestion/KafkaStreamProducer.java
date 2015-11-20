package br.edu.ufcg.analytics.wikitrends.ingestion;

import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Implementation of a simple Kafka producer
 *
 * @author Felipe Vieira - felipe29vieira@gmail.com
 */
public class KafkaStreamProducer implements StreamProducer {

	private KafkaProducer<String, String> producer;
	private String topic;
	private String key;

	/**
	 * Default constructor
	 * 
	 * @param configuration
	 */
	public KafkaStreamProducer(Configuration configuration) {
		topic = configuration.getString("wikitrends.ingestion.kafka.topic");
		key = configuration.getString("wikitrends.ingestion.kafka.key");

		Properties producerConfiguration = new Properties();
		
		producerConfiguration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				String.format("%s:%s", configuration.getString("wikitrends.ingestion.kafka.host"),
						configuration.getString("wikitrends.ingestion.kafka.port")));
		producerConfiguration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerConfiguration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		producer = new KafkaProducer<>(producerConfiguration);
	}

	/* (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.ingestion.StreamProducer#sendMessage(java.lang.String)
	 */
	@Override
	public void sendMessage(String message) {
		producer.send(new ProducerRecord<String, String>(topic, key, message));
	}
}