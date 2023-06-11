package producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class Producer implements Runnable {
    private static final String TOPIC_NAME = "my_topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092"; // Update with your Kafka bootstrap servers

    private final String filePath;
    private final long startLine;
    private final long endLine;
    private int messageCount = 0; // Counter for messages sent

    public Producer(String filePath, long startLine, long endLine) {
        this.filePath = filePath;
        this.startLine = startLine;
        this.endLine = endLine;
    }

    public void run() {
        KafkaProducer<String, String> producer = createKafkaProducer();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            long lineCount = 0;

            while ((line = reader.readLine()) != null) {
                if (lineCount >= startLine && lineCount <= endLine) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, line);
                    producer.send(record);
                    messageCount++; // Increment the message count
                }

                lineCount++;

                if (lineCount > endLine)
                    break;

                // Check for disconnection and reconnect if necessary
                if (lineCount % 100 == 0) {
                    producer.flush();
                    if (producer.metrics().size() == 0) {
                        System.out.println("Connection lost. Reconnecting...");
                        producer.close();
                        Thread.sleep(5000); // Wait for 5 seconds before reconnecting
                        producer = createKafkaProducer(); // Recreate the Kafka producer
                        System.out.println("Reconnected successfully.");
                    }
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
            System.out.println("Messages sent: " + messageCount); // Print the message count
        }
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(properties);
    }

    public static void main(String[] args) {
        String filePath = "/home/user/kafka/Kafka/datashet.csv";

        try {
            long totalLines = countTotalLines(filePath);
            long splitLine = totalLines / 2;

            Producer producer1 = new Producer(filePath, 1, splitLine);
            Producer producer2 = new Producer(filePath, splitLine + 1, totalLines);

            Thread producer1Thread = new Thread(producer1);
            Thread producer2Thread = new Thread(producer2);

            producer1Thread.start();
            producer2Thread.start();

            producer1Thread.join();
            producer2Thread.join();

            int totalMessagesSent = producer1.messageCount + producer2.messageCount;
            System.out.println("Total messages sent: " + totalMessagesSent); // Print the total message count
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static long countTotalLines(String filePath) throws IOException {
        long lineCount = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            // Skip the first line
            reader.readLine();

            while (reader.readLine() != null) {
                lineCount++;
            }
        }

        return lineCount;
    }
}
