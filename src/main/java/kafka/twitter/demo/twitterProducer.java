package kafka.twitter.demo;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class twitterProducer {

	Logger logger = LoggerFactory.getLogger(twitterProducer.class.getName());

	String consumerKey = "";
	String consumerSecret = "";
	String token = "";
	String secret = "";
	String kafkatopic = "twitter-tweets";

	public twitterProducer() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {

		new twitterProducer().run();

	}

	public void run() {

		logger.info("Start the twitter clinet");
		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

		final Client client = createTwitterClient(msgQueue);
		client.connect();

		// Create kafka Producer

		String bootstrapServer = "18.191.92.35:9092";
		String topic = "twitter-tweets";

		final KafkaProducer<String, String> producer = createKafkaProducer(bootstrapServer);

		// add shutdown hook

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {

			logger.info("Stopping twitter client...........");
			client.stop();
			logger.info("Stopping twitter producer.........");
			producer.close();

		}));

		// Send tweet to kafka

		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
			String msg = null;
			;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				client.stop();
			}

			if (msg != null) {
				ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, null, msg);

				producer.send(record, new Callback() {
					public void onCompletion(RecordMetadata recordMetadata, Exception e) {
						if (e == null) {

							logger.info("Received new Metadata.....\n" + "Topic: " + recordMetadata.topic() + "\n"
									+ "Partition:" + recordMetadata.partition() + "\n" + "Offset: "
									+ recordMetadata.offset());
						} else {
							logger.error("Producer record failed", e);
						}

					}

				});
			}

		}

	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);

		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

		// Optional: set up some followings and track terms
		List<String> terms = Lists.newArrayList("bitcoin");
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		// Create twitter client

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();

		return hosebirdClient;

	}

	public KafkaProducer<String, String> createKafkaProducer(String bootstrapServer) {

		// Set kafka Properties

		Properties props = new Properties();

		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "32767");
		props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");

		// Create kafka producer

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		return producer;

	}

}
