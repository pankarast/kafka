package consumers;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ReducerConsumer {
    private static final String BOOTSTRAP_SERVERS = "localhost:9093";
    private static final String INPUT_TOPIC = "quadrant-counts";
    private static final String OUTPUT_FILE = "/home/user/kafka/Kafka/quadrant-counts_output2.txt";
    private static Map<String, Integer> quadrantCounts;

    public static void main(String[] args) {
        // Set up consumer configuration
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "reducer_group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create Kafka consumer
        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {

            // Subscribe to input topic
            consumer.subscribe(Collections.singletonList(INPUT_TOPIC));
            System.out.println("Consumer subscribed to input topic: " + INPUT_TOPIC);

            // Map to store quadrant counts
            quadrantCounts = new HashMap<>();

            int messageCount = 0;
            // Start consuming messages
            while (messageCount < 675774) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    String quadrant = record.key();
                    int count = Integer.parseInt(record.value());

                    // Update quadrant count
                    quadrantCounts.put(quadrant, quadrantCounts.getOrDefault(quadrant, 0) + count);

                    System.out.println("Update quadrant count");
                    messageCount++;
                }
            }
            System.out.println("Messages counted: " + messageCount );
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Save output to text file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(OUTPUT_FILE))) {
            for (Map.Entry<String, Integer> entry : quadrantCounts.entrySet()) {
                String quadrant = entry.getKey();
                int count = entry.getValue();
                writer.write(quadrant + ": " + count);
                writer.newLine();
            }
            System.out.println("Output saved to " + OUTPUT_FILE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
