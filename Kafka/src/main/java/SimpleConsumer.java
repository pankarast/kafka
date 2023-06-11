import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleConsumer {

    public static void main(String[] args) {
        // Kafka broker configuration
        String bootstrapServers = "localhost:9092";

        // Kafka topic configuration
        String topicName = "my_topic";

        // Create Kafka consumer configuration
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // Quadrant boundaries
        double boundaryLatitude = 40.735923;
        double boundaryLongitude = -73.990294;

        // Quadrant counters
        int[] quadrantCounts = new int[4];

        // Create the Kafka consumer
        try (Consumer<String, String> consumer = new KafkaConsumer<>(config)) {
            // Subscribe to the topic
            consumer.subscribe(Collections.singleton(topicName));

            // Poll for records
            int messageCount = 0;
            boolean firstMessageSkipped = false;
            while (messageCount < 675773) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (var record : records) {
                    String message = record.value();
                    System.out.println(message);


                    // Split the message by commas
                    String[] tokens = message.split(",");
                    if (tokens.length >= 7) {
                        double latitude = Double.parseDouble(tokens[5].trim().replace("\"", ""));
                        double longitude = Double.parseDouble(tokens[6].trim().replace("\"", ""));
                        System.out.println("Latitude: " + latitude + ", Longitude: " + longitude);

                        // Calculate the quadrant for the route
                        int quadrant = calculateQuadrant(latitude, longitude, boundaryLatitude, boundaryLongitude);
                        System.out.println("Quadrant: " + quadrant);

                        // Increment the corresponding quadrant count
                        quadrantCounts[quadrant]++;
                    }

                    messageCount++;
                }
            }

            // Print the quadrant counts
            System.out.println("Quadrant Counts:");
            System.out.println("Quadrant 0: " + quadrantCounts[0]);
            System.out.println("Quadrant 1: " + quadrantCounts[1]);
            System.out.println("Quadrant 2: " + quadrantCounts[2]);
            System.out.println("Quadrant 3: " + quadrantCounts[3]);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static int calculateQuadrant(double latitude, double longitude, double boundaryLatitude, double boundaryLongitude) {
        if (latitude >= boundaryLatitude && longitude >= boundaryLongitude) {
            return 0; // Quadrant 0
        } else if (latitude >= boundaryLatitude && longitude < boundaryLongitude) {
            return 1; // Quadrant 1
        } else if (latitude < boundaryLatitude && longitude >= boundaryLongitude) {
            return 2; // Quadrant 2
        } else {
            return 3; // Quadrant 3
        }
    }
}
