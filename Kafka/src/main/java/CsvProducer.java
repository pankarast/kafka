import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class CsvProducer {

    public static void main(String[] args) {
        // Kafka broker configuration
        String bootstrapServers = "localhost:9092";

        // Kafka topic configuration
        String topicName = "my_topic";

        // CSV file path in the user's home directory
        String csvFilePath = "/home/user/kafka/Kafka/datashet.csv";

        // Create Kafka producer configuration
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // Create the Kafka producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            // Read the CSV file and publish messages
            BufferedReader reader = new BufferedReader(new FileReader(csvFilePath));
            String line;
            int messageCount = 0;
            while ((line = reader.readLine()) != null && messageCount < 10) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, line);
                producer.send(record);
                System.out.println(line);
                messageCount++;
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
