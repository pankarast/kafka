package consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    private static final String TOPIC_NAME = "popular-stations";
    private static final String BOOTSTRAP_SERVERS = "localhost:9094"; // Update with your Kafka bootstrap servers
    private static final int MESSAGE_LIMIT = 675774; // Maximum number of messages to print

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "popular-stations-mapper");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singleton(TOPIC_NAME));

        int messageCount = 0; // Counter for tracking the number of messages received

        try {
            while (messageCount < MESSAGE_LIMIT) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    // Print the received message
                    System.out.println("Key: "+ record.key() + " Value: "+ record.value());
                    messageCount++;

                }
            }
        } finally {
            consumer.close();
        }
    }
}
