import java.io.BufferedReader;
import java.io.FileReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class PopularBikeStations {
    public static void main(String[] args) {
        // Path and name of the CSV file
        String csvFile = "/home/user/kafka/Kafka/datashet.csv";

        // Create separate maps for each 6-hour interval
        Map<String, Integer> interval1Map = new HashMap<>(); // 00:00 - 05:59
        Map<String, Integer> interval2Map = new HashMap<>(); // 06:00 - 11:59
        Map<String, Integer> interval3Map = new HashMap<>(); // 12:00 - 17:59
        Map<String, Integer> interval4Map = new HashMap<>(); // 18:00 - 23:59

        try (BufferedReader reader = new BufferedReader(new FileReader(csvFile))) {
            String line;
            reader.readLine(); // Skip the first line with labels

            while ((line = reader.readLine()) != null) {
                String[] data = line.split(",");

                String startTimeString = data[1].replace("\"", ""); // Column "starttime"
                LocalDateTime startTime = LocalDateTime.parse(startTimeString, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

                int hour = startTime.getHour();
                String startStationId = data[3]; // Column "start station id"

                // Increase the count for the corresponding interval map
                if (hour >= 0 && hour <= 5) {
                    interval1Map.put(startStationId, interval1Map.getOrDefault(startStationId, 0) + 1);
                } else if (hour >= 6 && hour <= 11) {
                    interval2Map.put(startStationId, interval2Map.getOrDefault(startStationId, 0) + 1);
                } else if (hour >= 12 && hour <= 17) {
                    interval3Map.put(startStationId, interval3Map.getOrDefault(startStationId, 0) + 1);
                } else if (hour >= 18 && hour <= 23) {
                    interval4Map.put(startStationId, interval4Map.getOrDefault(startStationId, 0) + 1);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Find the top 10 bike stations for each interval
        findTopStations(interval1Map, "00:00 - 05:59");
        findTopStations(interval2Map, "06:00 - 11:59");
        findTopStations(interval3Map, "12:00 - 17:59");
        findTopStations(interval4Map, "18:00 - 23:59");
    }

    // Helper method to find the top 10 stations for a specific interval
    private static void findTopStations(Map<String, Integer> intervalMap, String intervalLabel) {
        PriorityQueue<Map.Entry<String, Integer>> popularStationsQueue = new PriorityQueue<>((a, b) -> b.getValue() - a.getValue());

        for (Map.Entry<String, Integer> entry : intervalMap.entrySet()) {
            popularStationsQueue.add(entry);
        }

        System.out.println("Top 10 Stations for Interval: " + intervalLabel);
        int count = 0;
        while (count < 10 && !popularStationsQueue.isEmpty()) {
            Map.Entry<String, Integer> entry = popularStationsQueue.poll();
            System.out.println("{" +intervalLabel + ", " + entry.getKey() + "} -> " + entry.getValue());
            count++;
        }
        System.out.println();
    }
}
