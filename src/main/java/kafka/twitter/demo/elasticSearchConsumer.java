package kafka.twitter.demo;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class elasticSearchConsumer {

	public elasticSearchConsumer() {
	}

	public static void main(String[] args) throws IOException {

		Logger logger = LoggerFactory.getLogger(elasticSearchConsumer.class.getName());

		String topic = "twitter_status_connect";
		String bootstrapServer = "13.59.167.188:9092";

		RestHighLevelClient client = createElasticseachclient();

		// Insert data to Elastic search

		KafkaConsumer<String, String> consumer = createkafkaConsumer(bootstrapServer);
		consumer.subscribe(Arrays.asList(topic));

		Integer retryCount = 0;
		Integer maxretryCount = 10;

		while (true) {

			if (maxretryCount > retryCount) {

				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

				logger.info("received " + records.count() + " records.......");
				int recordCount = records.count();
				BulkRequest bulkRequest = new BulkRequest();

				for (ConsumerRecord<String, String> record : records) {

					String id = record.topic() + "_" + record.partition() + "_" + record.offset();
					
					IndexRequest request = new IndexRequest("twitter", "tweets", id).source(record.value(),XContentType.JSON);
					
					bulkRequest.add(request);

				}

				if (recordCount > 0) {

					BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

					consumer.commitSync();
					logger.info("offset commited");
					retryCount = 0;
				}

				else {

					retryCount = retryCount + 1;
					logger.info("retryCount " + retryCount);
				}

			}

			else {

				logger.info("No message to consumer from topic");
				break;
			}
		}

		
		logger.info("closing the elasticsearch client and consumer");
		client.close();
		consumer.close();

	}

	public static RestHighLevelClient createElasticseachclient() {

		String hostName = "kafka-twitter-consum-193369953.us-east-1.bonsaisearch.net";
		String userName = "dt2yjwyh4m";
		String password = "44dcijesw7";

		final CredentialsProvider credentialProvider = new BasicCredentialsProvider();

		credentialProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));

		RestClientBuilder builder = RestClient.builder(new HttpHost(hostName, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {

					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						// TODO Auto-generated method stub
						return httpClientBuilder.setDefaultCredentialsProvider(credentialProvider);
					}
				});

		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}

	public static KafkaConsumer<String, String> createkafkaConsumer(String bootstrapServer) {

		// set Consumer Properties

		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "ES");
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "Java-Client");
		props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");
	

		// Create consumer

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		// Subscribe to the topic

		return consumer;
	}

}
