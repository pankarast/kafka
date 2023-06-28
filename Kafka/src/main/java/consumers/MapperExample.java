package consumers;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class MapperExample {
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

        // Start consuming messages
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String[] values = record.value().split(",");
                String startStationId = values[3];
                String latitude = values[5];
                String longitude = values[6];
                String interval = "\"" + mapToInterval(values[1].replace("\"", "")) + "\"";

                String key = interval + "#" + startStationId;
                String value = latitude + "," + longitude;

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(INTERMEDIATE_TOPIC, key, value);

                producer.send(producerRecord, (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.println("Produced record: " + key + ", " + value);
                    }
                });

            }
        }
    }

    private static String mapToInterval(String timestamp) {
        LocalDateTime dateTime = LocalDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        int hour = dateTime.getHour();
        String interval;

        if (hour >= 0 && hour < 6) {
            interval = "00:00 - 06:00";
        } else if (hour >= 6 && hour < 12) {
            interval = "06:00 - 12:00";
        } else if (hour >= 12 && hour < 18) {
            interval = "12:00 - 18:00";
        } else {
            interval = "18:00 - 24:00";
        }

        return interval;
    }
}
