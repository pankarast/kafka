package consumers;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;


import java.time.Duration;
import java.util.*;
import java.util.List;


import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReducerExample {
    private static final String INTERMEDIATE_TOPIC = "popular-stations";

    //variables for the image
    private static final String MAP_IMAGE_PATH = "/home/user/kafka/Kafka/map.png"; // Replace with the path to your map image
//    private static final double MAP_WIDTH = 1247.0;
//    private static final double MAP_HEIGHT = 1252.0;
//    private static final double TOP_LEFT_LATITUDE = 40.921814;
//    private static final double TOP_LEFT_LONGITUDE = -74.262689;
//    private static final double BOTTOM_RIGHT_LATITUDE = 40.495098;
//    private static final double BOTTOM_RIGHT_LONGITUDE = -73.699763;

    private static final double MAP_WIDTH = 1000.0;
    private static final double MAP_HEIGHT = 1000.0;
    private static final double TOP_LEFT_LATITUDE = 40.81334;
    private static final double TOP_LEFT_LONGITUDE = -74.05160;
    private static final double BOTTOM_RIGHT_LATITUDE = 40.68456;
    private static final double BOTTOM_RIGHT_LONGITUDE = -73.88095;
    private static final Color MARKER_COLOR = Color.RED;
    private static final int MARKER_SIZE = 5;

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
            BufferedImage mapImage = loadMapImage();
            for (Map.Entry<String, StationInfo> stationEntry : sortedStations) {
                String station = stationEntry.getKey();
                StationInfo stationInfo = stationEntry.getValue();

                System.out.println("Station: " + station + ", Count: " + stationInfo.getCount()
                        + ", Latitude: " + stationInfo.getLatitude() + ", Longitude: " + stationInfo.getLongitude());


                double latitude = stationInfo.getLatitude();
                double longitude = stationInfo.getLongitude();

                // Add marker for the station on the map image
                mapImage = addMarker(mapImage, latitude, longitude);


                count++;
                if (count >= 10) {
                    break;
                }
            }

            System.out.println();

            // Save the image for the interval
            String imagePath = "/home/user/kafka/Kafka/" + interval + ".png";

            saveImage(mapImage, imagePath);
        }

        consumer.close();
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
