package br.edu.ufcg.analytics.wikitrends.ingestion;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import com.google.gson.JsonElement;

import io.socket.IOAcknowledge;
import io.socket.IOCallback;
import io.socket.SocketIO;
import io.socket.SocketIOException;

public class Ingestor {

	private SocketIO wikimediaSocket;
	private CustomKafkaProducer kafkaProducer;

	public Ingestor() throws IOException {
		this.wikimediaSocket = new SocketIO("http://stream.wikimedia.org:80/rc");
		this.kafkaProducer = new CustomKafkaProducer();
	}

	public void start() throws MalformedURLException, InterruptedException, ExecutionException {
		// Avoiding logs
		Logger l0 = Logger.getLogger("");
		l0.removeHandler(l0.getHandlers()[0]);
		
		kafkaProducer.initializeKafkaProducer();
		
		wikimediaSocket.connect(new IOCallback() {
			@Override
			public void onMessage(String data, IOAcknowledge ack) {
				System.out.println("Server said: " + data);
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
				wikimediaSocket.emit("subscribe", "*");
				System.out.println("Connection established");
				Thread socketThread = new Thread() {
					public void run() {
						while (true) {

						}
					}
				};

				socketThread.start();

			}

			@Override
			public void on(String arg0, IOAcknowledge arg1, JsonElement... arg2) {
				try {
					System.out.println(arg2[0].toString());
					kafkaProducer.sendMessage(arg2[0].toString());
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}

			@Override
			public void onMessage(JsonElement arg0, IOAcknowledge arg1) {

			}
		});
	}

	public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException, ExecutionException {
		Ingestor ingestor = new Ingestor();
		ingestor.start();
	}
}
