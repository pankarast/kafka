import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class KafkaProducerExample {
    private static final String TOPIC = "my_topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 10; i++) {
                String message = "Message " + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, message);
                producer.send(record);
                System.out.println("Sent message: " + message);
            }
        }
    }
}
