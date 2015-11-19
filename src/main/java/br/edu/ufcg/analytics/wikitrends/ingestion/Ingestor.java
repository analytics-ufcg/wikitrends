package br.edu.ufcg.analytics.wikitrends.ingestion;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Logger;

import org.apache.commons.configuration.Configuration;

import com.google.gson.JsonElement;

import br.edu.ufcg.analytics.wikitrends.WikiTrendsConfigurationException;
import br.edu.ufcg.analytics.wikitrends.WikiTrendsProcess;
import io.socket.IOAcknowledge;
import io.socket.IOCallback;
import io.socket.SocketIO;
import io.socket.SocketIOException;

/**
 * TODO comment me!
 * @author Felipe
 */
public class Ingestor implements WikiTrendsProcess {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1133137012061009751L;
	
	private StreamProducer producer;
	
	private SocketIO dataSource;

	/**
	 * Default constructor
	 * 
	 * @param configuration
	 */
	public Ingestor(Configuration configuration){
		this.producer = new KafkaStreamProducer(configuration);
		try {
			dataSource = new SocketIO(new URL(configuration.getString("wikitrends.ingestion.wikimedia.stream")));
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
			}

			@Override
			public void onConnect() {
				dataSource.emit("subscribe", "*");
				System.out.println("Connection established");
			}

			@Override
			public void on(String eventName, IOAcknowledge eventAcknowledge, JsonElement... eventArguments) {
				producer.sendMessage(eventArguments[0].toString());
			}			
		});
	}
}
