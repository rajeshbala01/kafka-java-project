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

		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "18.216.131.173:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "32767"); //batch records together into fewer requests whenever multiple records are being sent to the same partition
		props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10"); //This setting gives the upper bound on the delay for batching: once we get BATCH_SIZE_CONFIG worth of records for a partition it will be sent immediately regardless of this setting

		// Create kafka producer

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		// Create Producer Record

		for (int i = 0; i < 10; i++) {
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(
					"demo-producer",
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
		
		logger.info("CLosing the Producer");

	}
	
}
