package br.edu.ufcg.analytics.wikitrends.ingestion;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.UUID;
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
public class Ingestor implements WikiTrendsProcess {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1133137012061009751L;
	
	private StreamProducer producer;
	
	private SocketIO dataSource;
	
	private int trials;

	private String wikimediaStreamURL;

	/**
	 * Default constructor
	 * 
	 * @param configuration
	 */
	public Ingestor(Configuration configuration){
		this.producer = new KafkaStreamProducer(configuration);
		this.trials = 10;
		wikimediaStreamURL = configuration.getString("wikitrends.ingestion.wikimedia.stream");
		
		try {
			dataSource = new SocketIO(new URL(wikimediaStreamURL));
		} catch (MalformedURLException e) {
			e.printStackTrace();
			throw new WikiTrendsConfigurationException(e);
		}
	}

	/* (non-Javadoc)
	 * @see br.edu.ufcg.analytics.wikitrends.WikiTrendsProcess#run()
	 */
	@Override
	public void run(){
		// Avoiding logs
		// FIXME is there a way of doing this for the whole application?
		Logger l0 = Logger.getLogger("");
		l0.removeHandler(l0.getHandlers()[0]);
		 
		dataSource.connect(new IOCallback() {
			@Override
			public void onMessage(String data, IOAcknowledge ack) {
				System.out.println("Server said: " + data);
			}
			
			@Override
			public void onMessage(JsonElement arg0, IOAcknowledge arg1) {
				System.out.println("Server said: " + arg0);
			}

			@Override
			public void onError(SocketIOException socketIOException) {
				System.out.println("an Error occured");
				socketIOException.printStackTrace();
			}

			@Override
			public void onDisconnect() {
				System.out.println("Connection terminated.");
				while(trials > 0){
					trials--;
					try {
						Thread.sleep(10000);
						System.out.printf("Attempting to connect... (%d more trials before giving up)/n", trials);
						dataSource = new SocketIO(new URL(wikimediaStreamURL));
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
				System.out.println("Connection established");
			}

			@Override
			public void on(String eventName, IOAcknowledge eventAcknowledge, JsonElement... eventArguments) {
				JsonObject jsonObject = eventArguments[0].getAsJsonObject();
				String uuid = UUID.randomUUID().toString();
				jsonObject.addProperty("uuid", uuid);
				
				producer.sendMessage(uuid, jsonObject.toString());
			}			
		});
	}
}
