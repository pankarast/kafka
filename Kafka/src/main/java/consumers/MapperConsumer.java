package consumers;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;

public class MapperConsumer {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String INPUT_TOPIC = "my_topic";
    private static final String OUTPUT_TOPIC = "quadrant-counts";

    public static void main(String[] args) {
        // Set up consumer configuration
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Set up producer configuration
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Kafka consumer and producer
        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
             Producer<String, String> producer = new KafkaProducer<>(producerProps)) {

            // Subscribe to input topic
            consumer.subscribe(Collections.singletonList(INPUT_TOPIC));
            System.out.println("Consumer subscribed to input topic: " + INPUT_TOPIC);

            int messageCount = 0;
            // Start consuming messages
            while (messageCount < 675774) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    String[] fields = record.value().split(",");
                    double latitude = Double.parseDouble(fields[5].trim().replace("\"", ""));
                    double longitude = Double.parseDouble(fields[6].trim().replace("\"", ""));
                    String quadrant = calculateQuadrant(latitude, longitude);
                    producer.send(new ProducerRecord<>(OUTPUT_TOPIC, quadrant, "1"));
                    System.out.println("Produced message to output topic: " + OUTPUT_TOPIC);
                    messageCount++;
                }

            }
            System.out.println("Total messages sent " + messageCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String calculateQuadrant(double latitude, double longitude) {
        if (latitude >= 40.735923 && longitude >= -73.990294) {
            return "Q1";
        } else if (latitude >= 40.735923 && longitude < -73.990294) {
            return "Q2";
        } else if (latitude < 40.735923 && longitude >= -73.990294) {
            return "Q3";
        } else {
            return "Q4";
        }
    }
}
