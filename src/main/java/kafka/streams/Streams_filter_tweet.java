package kafka.streams;

import java.util.Properties;


import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.google.gson.JsonParser;


public class Streams_filter_tweet {

	public static void main(String[] args) {
		
		// Create properties.
		
		Properties props = new Properties();

		props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"18.191.92.35:9092");
		props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"kafka-stream-twitter");
		props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,StringSerde.class.getName());
		props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,StringSerde.class.getName());
		
		//Create topology
		
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		
		// input topic and filter logic
		
		KStream<String, String> inputTopics =  streamsBuilder.stream("twitter-tweets");
		KStream<String, String> filteredStream = inputTopics.filter(
				(k, jsonTweet) -> extractTwitterFollower(jsonTweet) >1000);
				
		filteredStream.to("important-tweets");
		
		//build topology
		
		KafkaStreams kafkaStreams = new KafkaStreams(
				streamsBuilder.build(),
				props
				);
		
		//start our streams application
		
		kafkaStreams.start();

	}
	private static JsonParser jsonParser = new JsonParser();
	
	public static Integer extractTwitterFollower(String tweetJson) {
		
		try {
		return jsonParser.parse(tweetJson)
				.getAsJsonObject()
				.get("user")
				.getAsJsonObject()
				.get("followers_count")
				.getAsInt();
		}
		catch (NullPointerException e) {
			
			return 0;
		}
	}

}
