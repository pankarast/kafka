import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class PopularBikeStations2 {

    private static final String MAP_IMAGE_PATH = "/home/user/kafka/Kafka/map.png"; // Replace with the path to your map image
    private static final double MAP_WIDTH = 1247.0;
    private static final double MAP_HEIGHT = 1252.0;
    private static final double TOP_LEFT_LATITUDE = 40.921814;
    private static final double TOP_LEFT_LONGITUDE = -74.262689;
    private static final double BOTTOM_RIGHT_LATITUDE = 40.495098;
    private static final double BOTTOM_RIGHT_LONGITUDE = -73.699763;
    private static final Color MARKER_COLOR = Color.MAGENTA;
    private static final int MARKER_SIZE = 10;

    public static void main(String[] args) {
        // Path and name of the CSV file
        String csvFile = "/home/user/kafka/Kafka/datashet.csv";

        // Create separate maps for each 6-hour interval
        Map<String, Integer> interval1Map = new HashMap<>(); // 00:00 - 05:59
        Map<String, Integer> interval2Map = new HashMap<>(); // 06:00 - 11:59
        Map<String, Integer> interval3Map = new HashMap<>(); // 12:00 - 17:59
        Map<String, Integer> interval4Map = new HashMap<>(); // 18:00 - 23:59

        // Create a map to store top stations with their latitude and longitude
        Map<String, Map<String, String>> topStationsMap = new HashMap<>();

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

                // Find the top 10 bike stations for each interval and store their latitude and longitude
                findTopStations(interval1Map, "00:00 - 05:59", data, topStationsMap);
                findTopStations(interval2Map, "06:00 - 11:59", data, topStationsMap);
                findTopStations(interval3Map, "12:00 - 17:59", data, topStationsMap);
                findTopStations(interval4Map, "18:00 - 23:59", data, topStationsMap);
            }

            // Load the map image
            BufferedImage mapImage = loadMapImage();

            // Add markers for each station to the map image
            for (Map.Entry<String, Map<String, String>> entry : topStationsMap.entrySet()) {
                String stationId = entry.getKey();
                Map<String, String> stationData = entry.getValue();
                double latitude = Double.parseDouble(stationData.get("latitude"));
                double longitude = Double.parseDouble(stationData.get("longitude"));

                mapImage = addMarker(mapImage, latitude, longitude);
            }

            // Save the modified image with all the markers
            String outputFilePath = "/home/user/kafka/Kafka/output.png"; // Replace with the desired output path and filename
            saveImage(mapImage, outputFilePath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    // Helper method to find the top 10 stations for a specific interval and store their latitude and longitude
    private static void findTopStations(Map<String, Integer> intervalMap, String intervalLabel, String[] data, Map<String, Map<String, String>> topStationsMap) {
        PriorityQueue<Map.Entry<String, Integer>> popularStationsQueue = new PriorityQueue<>((a, b) -> b.getValue() - a.getValue());

        for (Map.Entry<String, Integer> entry : intervalMap.entrySet()) {
            popularStationsQueue.add(entry);
        }

        int count = 0;
        while (count < 10 && !popularStationsQueue.isEmpty()) {
            Map.Entry<String, Integer> entry = popularStationsQueue.poll();
            String stationId = entry.getKey();
            int popularity = entry.getValue();

            String latitude = data[5].replace("\"", ""); // Column "start station latitude"
            String longitude = data[6].replace("\"", ""); // Column "start station longitude"

            // Create a nested map for the station with latitude and longitude
            Map<String, String> stationData = new HashMap<>();
            stationData.put("latitude", latitude);
            stationData.put("longitude", longitude);

            // Add the station data to the top stations map
            topStationsMap.put(stationId, stationData);

            count++;
        }
    }


    private static BufferedImage loadMapImage() {
        try {
            return ImageIO.read(new File(MAP_IMAGE_PATH));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static BufferedImage addMarker(BufferedImage mapImage, double latitude, double longitude) {
        double x = (longitude - TOP_LEFT_LONGITUDE) * (MAP_WIDTH / (BOTTOM_RIGHT_LONGITUDE - TOP_LEFT_LONGITUDE));
        double y = (TOP_LEFT_LATITUDE - latitude) * (MAP_HEIGHT / (TOP_LEFT_LATITUDE - BOTTOM_RIGHT_LATITUDE));

        Graphics2D g2d = mapImage.createGraphics();
        g2d.setColor(MARKER_COLOR);
        g2d.fillOval((int) x, (int) y, MARKER_SIZE, MARKER_SIZE);
        g2d.dispose();

        return mapImage;
    }

    private static void saveImage(BufferedImage image, String filePath) {
        try {
            File outputFile = new File(filePath);
            ImageIO.write(image, "png", outputFile);
            // For JPEG, use the following line instead:
            // ImageIO.write(image, "jpeg", outputFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
