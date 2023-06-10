import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaProducerConsumerExample {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        // Create producer properties
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        // Create Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton("my_topic"));

        // Start producer thread
        Thread producerThread = new Thread(() -> {
            try {
                // Send messages
                for (int i = 0; i < 10; i++) {
                    String message = "Message " + i;
                    producer.send(new ProducerRecord<>("my_topic", message), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception == null) {
                                System.out.println("Message sent successfully: " + metadata.topic() + ", " +
                                        metadata.partition() + ", " + metadata.offset());
                            } else {
                                System.out.println("Error sending message: " + exception.getMessage());
                            }
                        }
                    });
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                producer.close();
            }
        });

        // Start consumer thread
        Thread consumerThread = new Thread(() -> {
            try {
                // Consume messages
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println("Received message: " + record.value());
                    }
                }
            } finally {
                consumer.close();
            }
        });

        // Start producer and consumer threads
        producerThread.start();
        consumerThread.start();

        // Wait for threads to finish
        try {
            producerThread.join();
            consumerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
