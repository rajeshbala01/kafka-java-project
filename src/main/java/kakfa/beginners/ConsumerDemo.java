package kakfa.beginners;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {
	
	public static void main(String[] args) {
		
		final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
		String topic = "twitter_status_connect";
		
		//set Consumer Properties
		
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "18.216.133.220:9092");
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "ES");
		props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "Java-Client");
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");
		
		//Create consumer
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		
		// Subcribe to the topic
		
		consumer.subscribe(Collections.singleton(topic));
		
		/*or Assign and seek
		
		TopicPartition partitions = new TopicPartition(topic, 0);
		consumer.assign(Collections.singleton(partitions));
		
		consumer.seek(partitions, 10);*/
		
		
		//Poll and commit the data
		
		Integer retryCount = 0;
		Integer maxretryCount = 5;
		
		while(true) {
		
		if (maxretryCount>retryCount) {
		
		ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
		
		if (records.count() > 0) {
		
		for(ConsumerRecord<String, String> record: records) {
				
				logger.info(record.key()+", "+ record.offset()+","+record.timestamp());
			}
		}
		else {
			
			retryCount = retryCount + 1;
			
			logger.info("retryCount: " + retryCount);
		}
		}
		else {
			
			logger.info("No message to consumer, closing consumer");
			consumer.close();
			break;
			
		}
			
				
	}
		
		
}
	
}
