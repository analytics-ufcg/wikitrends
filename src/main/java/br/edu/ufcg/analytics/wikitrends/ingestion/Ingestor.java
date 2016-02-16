package br.edu.ufcg.analytics.wikitrends.ingestion;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.UUID;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.configuration.Configuration;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import br.edu.ufcg.analytics.wikitrends.WikiTrendsConfigurationException;
import br.edu.ufcg.analytics.wikitrends.WikiTrendsProcess;
import io.socket.IOAcknowledge;
import io.socket.IOCallback;
import io.socket.SocketIO;
import io.socket.SocketIOException;

/**
 * Conversion between a RCStream pipeline and a Kafka Producer
 * 
 * @author Felipe Vieira - felipe29vieira@gmail.com
 */
public class Ingestor implements WikiTrendsProcess, IOCallback {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1133137012061009751L;
	
	private StreamProducer producer;
	
	private SocketIO dataSource;
	
	private int trials;

	private String wikimediaStreamURL;
	
	private Logger logger;

	/**
	 * Default constructor
	 * 
	 * @param configuration
	 */
	public Ingestor(Configuration configuration){
		this.producer = new KafkaStreamProducer(configuration);
		this.trials = 10;
		wikimediaStreamURL = configuration.getString("wikitrends.ingestion.wikimedia.stream");

		configLoggers();

		try {
			dataSource = new SocketIO(new URL(wikimediaStreamURL));
		} catch (MalformedURLException e) {
			e.printStackTrace();
			throw new WikiTrendsConfigurationException(e);
		}
	}

	private void configLoggers() {
		Logger rootLogger = Logger.getLogger("");
		Handler[] handlers = rootLogger.getHandlers();
		rootLogger.removeHandler(rootLogger.getHandlers()[0]);
		if (handlers[0] instanceof ConsoleHandler) {
			rootLogger.removeHandler(handlers[0]);
		}

		Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
	    logger.setLevel(Level.INFO);
	    
	    this.logger = Logger.getLogger(Ingestor.class.getName());
	}

	/* (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.WikiTrendsProcess#run()
	 */
	@Override
	public void run(String... args){
		dataSource.connect(this);
	}

	@Override
	public void onDisconnect() {
		logger.info("Ingestor has been disconnected.");
		while(trials > 0){
			trials--;
			try {
				Thread.sleep(10000);
				System.out.printf("Attempting to connect... (%d more trials before giving up)/n", trials);
				dataSource = new SocketIO(new URL(wikimediaStreamURL));
				this.run();
				return;
			} catch (MalformedURLException | InterruptedException e) {
				e.printStackTrace();
				throw new WikiTrendsConfigurationException(e);
			}
		}
	}

	@Override
	public void onConnect() {
		trials = 10;
		dataSource.emit("subscribe", "*");
		logger.info("Connection established");
	}

	@Override
	public void onMessage(String data, IOAcknowledge ack) {
		logger.info("Server said: " + data);
	}

	@Override
	public void onMessage(JsonElement json, IOAcknowledge ack) {
		logger.info("Server said: " + json);
	}

	@Override
	public void on(String event, IOAcknowledge ack, JsonElement... args) {
		JsonObject jsonObject = args[0].getAsJsonObject();
		String uuid = UUID.randomUUID().toString();
		jsonObject.addProperty("uuid", uuid);
		
		producer.sendMessage(uuid, jsonObject.toString());
	}

	@Override
	public void onError(SocketIOException socketIOException) {
		logger.info("An error occured on Ingestor connection. Will attempt to recover");
		socketIOException.printStackTrace();
		onDisconnect();
	}
}
