import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.utils.Time;

import java.util.Properties;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import java.io.IOException;


public class KafkaBrokerExample {

    public static void main(String[] args) throws Exception {
        startKafkaBroker(0, 9092, "/tmp/kafka-logs-0");
        startKafkaBroker(1, 9093, "/tmp/kafka-logs-1");
        startKafkaBroker(2, 9094, "/tmp/kafka-logs-2");

        createTopic("my_topic", 2, 3);
        createTopic("quadrant-counts", 2, 3);

        // Keep the program running until interrupted
        Thread.sleep(Long.MAX_VALUE);
    }

    private static void startKafkaBroker(int brokerId, int port, String logDir) throws IOException {
        Properties properties = new Properties();
        properties.setProperty("broker.id", String.valueOf(brokerId));
        properties.setProperty("listeners", "PLAINTEXT://localhost:" + port);
        properties.setProperty("log.dirs", logDir);
        properties.setProperty("zookeeper.connect", "localhost:2181");

        KafkaConfig kafkaConfig = new KafkaConfig(properties);
        KafkaServer kafkaServer = new KafkaServer(kafkaConfig, Time.SYSTEM, scala.Option.empty(), true);

        kafkaServer.startup();
        System.out.println("Kafka broker with ID " + brokerId + " started on port " + port);
    }

    private static void createTopic(String topicName, int numPartitions, int replicationFactor) throws ExecutionException, InterruptedException {
        Properties adminProperties = new Properties();
        adminProperties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(adminProperties)) {
            NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) replicationFactor);
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            System.out.println("Topic '" + topicName + "' created with " + numPartitions + " partitions and replication factor " + replicationFactor);
        }
    }
}
