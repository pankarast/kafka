package consumers;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.util.*;

public class PopularStationsReducer {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String INTERMEDIATE_TOPIC = "popular-stations";
    private static final int TOP_N_STATIONS = 10;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "popular-stations-reducer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(INTERMEDIATE_TOPIC));

        try {
            Map<String, Integer> stationCounts = new HashMap<>();
            Map<String, List<String>> stationCoordinates = new HashMap<>();

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    String[] fields = record.key().split("-");
                    String startTime = fields[0];
                    String startStationID = fields[1];

                    // Increment count for each interval-station pair
                    String intervalStationKey = startTime + "-" + startStationID;
                    int count = stationCounts.getOrDefault(intervalStationKey, 0) + 1;
                    stationCounts.put(intervalStationKey, count);

                    // Store station coordinates
                    String latitude = getLatitudeFromKey(record.key());
                    String longitude = getLongitudeFromKey(record.key());
                    List<String> coordinates = stationCoordinates.getOrDefault(startStationID, new ArrayList<>());
                    coordinates.add(latitude + "-" + longitude);
                    stationCoordinates.put(startStationID, coordinates);
                }

                // Sort stations by count in descending order
                List<Map.Entry<String, Integer>> sortedStations = new ArrayList<>(stationCounts.entrySet());
                sortedStations.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));

                // Print the top N stations with coordinates
                int count = 0;
                for (Map.Entry<String, Integer> entry : sortedStations) {
                    String intervalStationKey = entry.getKey();
                    String startTime = getStartTimeFromKey(intervalStationKey);
                    String startStationID = getStationIDFromKey(intervalStationKey);
                    List<String> coordinates = stationCoordinates.get(startStationID);

                    System.out.println(startTime + " -> " + startStationID + " (" + entry.getValue() + ")");
                    if (coordinates != null) {
                        for (String coordinate : coordinates) {
                            System.out.println("  Latitude: " + getLatitudeFromCoordinate(coordinate));
                            System.out.println("  Longitude: " + getLongitudeFromCoordinate(coordinate));
                        }
                    }

                    count++;
                    if (count >= TOP_N_STATIONS) {
                        break;
                    }
                }

                // Clear data for the next batch
                stationCounts.clear();
                stationCoordinates.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    private static String getStartTimeFromKey(String key) {
        return key.split("-")[0];
    }

    private static String getStationIDFromKey(String key) {
        return key.split("-")[1];
    }

    private static String getLatitudeFromKey(String key) {
        return key.split("-")[2];
    }

    private static String getLongitudeFromKey(String key) {
        return key.split("-")[3];
    }

    private static String getLatitudeFromCoordinate(String coordinate) {
        return coordinate.split("-")[0];
    }

    private static String getLongitudeFromCoordinate(String coordinate) {
        return coordinate.split("-")[1];
    }
}
