package kakfa.beginners;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {

	public static void main(String[] args) {

		final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

		// Set kafka Properties

		Properties props = new Properties();

		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "18.216.133.220:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "32767");
		props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10");

		// Create kafka producer

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		// Create Producer Record

		for (int i = 0; i < 10; i++) {
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(
					"twitter_status_connect",
					"id_"+ Integer.toString(i), 
					"Test message - " + Integer.toString(i));

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

		producer.close();

	}
	
}
