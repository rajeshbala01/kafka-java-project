package kafka.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Streams_filter_mySql {

	public static void main(String[] args) {

		Logger logger = LoggerFactory.getLogger(Streams_filter_mySql.class.getName());

		// Create properties.

		Properties props = new Properties();

		props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "18.216.131.173:9092");
		props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "customer-location-stream");
		props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class.getName());
		props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class.getName());

		// Create topology

		StreamsBuilder streamsBuilder = new StreamsBuilder();

		// input topic and filter logic

		KStream<String, String> inputTopics = streamsBuilder.stream("mysql-Customer");
		KStream<String, String> filteredStream = inputTopics.filter((k, v) -> filterCity(v));

		filteredStream.to("Customer-location");
		
		// build topology

		KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);

		// start our streams application

		kafkaStreams.start();

	}

	private static JsonParser jsonParser = new JsonParser();

	public static Boolean filterCity(String v) {

		try {

			Logger logger = LoggerFactory.getLogger(Streams_filter_mySql.class.getName());

			String filter = jsonParser.parse(v).getAsJsonObject().get("payload").getAsJsonObject().get("City")
					.getAsString();
			
			logger.info(filter);

			if (filter.equalsIgnoreCase("Chennai")) {
				
				return true;
			}
			
			else {
				
				return false;
			}
			
		} catch (NullPointerException e) {

			return null;
		}
	}

}
