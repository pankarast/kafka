import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

public class BikeStationsMap {
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
        double latitude = 40.7128; // Latitude of the marker
        double longitude = -74.0060; // Longitude of the marker

        BufferedImage mapImage = loadMapImage();
        BufferedImage markedImage = addMarker(mapImage, latitude, longitude);

        String outputFilePath = "/home/user/kafka/Kafka/output.png"; // Replace with the desired output path and filename
        saveImage(markedImage, outputFilePath);
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
