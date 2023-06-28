package consumers;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.util.*;

public class PopularStationsMapper {

    private static final String BOOTSTRAP_SERVER_CONSUMER = "localhost:9092";
    private static final String BOOTSTRAP_SERVER_PRODUCER = "localhost:9094";
    private static final String INPUT_TOPIC = "my_topic";
    private static final String INTERMEDIATE_TOPIC = "popular-stations";

    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_CONSUMER);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "popular-stations-mapper");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(INPUT_TOPIC));

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_PRODUCER);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        System.out.println("Consumer subscribed to input topic: " + INPUT_TOPIC);
        int messageCount = 0;

        try {
            while (messageCount < 675774) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    String[] fields = record.value().split(",");
                    String startTime = fields[1];
                    String startStationID = fields[3];
                    String latitude = fields[5];
                    String longitude = fields[6];

                    // Emit intermediate key-value pairs
                    String intermediateKey = startTime + "-" + startStationID + "-" + latitude + "-" + longitude;

                    ProducerRecord<String, String> intermediateRecord = new ProducerRecord<>(INTERMEDIATE_TOPIC, intermediateKey, "1");
                    producer.send(intermediateRecord);
                    System.out.println("Produced message to output topic: " + INTERMEDIATE_TOPIC);
                    messageCount++;
                }

            }       System.out.println("Total messages sent " + messageCount);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            producer.close();
        }
    }
}
