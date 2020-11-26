package kakfa.beginners;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateTopic {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		
		Logger logger = LoggerFactory.getLogger(CreateTopic.class.getName());

		// Set properties
		Properties config = new Properties();
		config.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "18.216.131.173:9092");

		// Create admin client
		AdminClient admin = AdminClient.create(config);

		// creating new topic
		logger.info("Creating the Topic");
		
		NewTopic newTopic = new NewTopic("my-new-topic", 3, (short) 1);
		
		admin.createTopics(Collections.singleton(newTopic));

		// listing
		admin.listTopics().names().get().forEach(System.out::println);

	}

}
