package consumers;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.util.*;

public class ReducerExample {
    private static final String INTERMEDIATE_TOPIC = "popular-stations";
    private static final int MESSAGE_COUNT = 675774;

    private static class StationInfo {
        private int count;
        private double latitude;
        private double longitude;

        public StationInfo(int count, double latitude, double longitude) {
            this.count = count;
            this.latitude = latitude;
            this.longitude = longitude;
        }

        public int getCount() {
            return count;
        }

        public double getLatitude() {
            return latitude;
        }

        public double getLongitude() {
            return longitude;
        }
    }

    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "popular-stations-reducer");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(INTERMEDIATE_TOPIC));

        // Data structure to store interval-station information
        Map<String, Map<String, StationInfo>> intervalStations = new HashMap<>();

        // Set the timeout duration to 10 seconds
        Duration timeoutDuration = Duration.ofSeconds(30);
        long startTime = System.currentTimeMillis();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String value = record.value();
                System.out.println("Key: " + key + ", Value: " + value);
                // Extract interval and station ID from the key
                String[] keyParts = key.split("#");
                String interval = keyParts[0].trim();
                String stationId = keyParts[1].trim();

                // Extract latitude, longitude, and count from the value
                String[] valueParts = value.split(",");
                double latitude = Double.parseDouble(valueParts[0].trim().replace("\"", ""));
                double longitude = Double.parseDouble(valueParts[1].trim().replace("\"", ""));
                int count = 1;

                // Update the interval-station information
                Map<String, StationInfo> stationInfoMap = intervalStations.computeIfAbsent(interval, k -> new HashMap<>());
                StationInfo stationInfo = stationInfoMap.get(stationId);
                if (stationInfo == null) {
                    stationInfo = new StationInfo(count, latitude, longitude);
                    stationInfoMap.put(stationId, stationInfo);
                } else {
                    stationInfo.count += count;
                }

//                System.out.println("Key: " + key + ", Value: " + value);

                // Check if the timeout duration has elapsed
                long elapsedTime = System.currentTimeMillis() - startTime;
                if (elapsedTime >= timeoutDuration.toMillis()) {
                    break;
                }
            }

            // Check if the timeout duration has elapsed
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (elapsedTime >= timeoutDuration.toMillis()) {
                break;
            }
        }

        // Calculate and print the top 10 stations for each interval
        for (Map.Entry<String, Map<String, StationInfo>> entry : intervalStations.entrySet()) {
            String interval = entry.getKey();
            Map<String, StationInfo> stationInfoMap = entry.getValue();

            List<Map.Entry<String, StationInfo>> sortedStations = new ArrayList<>(stationInfoMap.entrySet());
            sortedStations.sort(Comparator.comparingInt(e -> e.getValue().getCount()));
            Collections.reverse(sortedStations);

            System.out.println("Interval: " + interval);

            int count = 0;
            for (Map.Entry<String, StationInfo> stationEntry : sortedStations) {
                String station = stationEntry.getKey();
                StationInfo stationInfo = stationEntry.getValue();

                System.out.println("Station: " + station + ", Count: " + stationInfo.getCount()
                        + ", Latitude: " + stationInfo.getLatitude() + ", Longitude: " + stationInfo.getLongitude());

                count++;
                if (count >= 10) {
                    break;
                }
            }

            System.out.println();
        }

        consumer.close();
    }
}
