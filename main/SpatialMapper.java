import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class SpatialMapper {

    /**
     This Mapper class is responsible for mapping points to their respective grids.
     It uses a window to filter out points that are outside the specified bounds.
     The window is defined as a comma-separated string containing four integers:
            the x and y coordinates of the top-left corner, followed by the x and y coordinates
            of the bottom-right corner.
     */
    public static class MapPoints extends Mapper<Object, Text, Text, Text> {
        private List<Grid> grids;
        private int windowTopLeftX;
        private int windowTopLeftY;
        private int windowBottomRightX;
        private int windowBottomRightY;

        /**
         This method parses the window string and sets the values of the window attributes.
         @param window The window string.
         @return An array containing the window attributes (top-left x, top-left y, bottom-right x, bottom-right y).
         */
        private int[] getWindowAttributes(String window) {
            int[] windowAtt = new int[4];
            if (window != null && !window.isEmpty()) {
                String[] windowParts = window.split(",");
                for (int i = 0; i < windowAtt.length; i++) {
                    windowAtt[i] = Integer.parseInt(windowParts[i]);
                }
            }
            return windowAtt;
        }

        /**
         This method is called once at the beginning of the map task.
         It sets up the list of Grid objects based on the partitions created by the DivideIntoBlocks class.
         It also sets the window attributes using the window string passed in the job configuration.
         */
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String window = conf.get("window");
            int[] windowAtt = getWindowAttributes(window);
            windowTopLeftX = windowAtt[0];
            windowTopLeftY = windowAtt[1];
            windowBottomRightX = windowAtt[2];
            windowBottomRightY = windowAtt[3];
            List<String> gridPartitions = DivideIntoBlocks.createBlocks(10000, 2500);
            grids = Arrays.asList(new Grid[gridPartitions.size()]);
            for (int i = 0; i < gridPartitions.size(); i++) {
                String[] gridDef = gridPartitions.get(i).split(",");
                int x1 = Integer.parseInt(gridDef[1]);
                int y1 = Integer.parseInt(gridDef[2]);
                int x2 = Integer.parseInt(gridDef[3]);
                int y2 = Integer.parseInt(gridDef[4]);
                grids.set(i, new Grid(gridDef[0], x1, y1, x2, y2));
            }

        }

        /**
         The method below maps each point to its corresponding grid.
         It checks whether the point is contained in each grid and whether it is inside the specified window.
         If the point satisfies these conditions, it is written to the output context with the grid id as the key.
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Point point = Point.fromString(value.toString());
            for (Grid grid : grids) {
                if (grid.contains(point) && point.x >= windowTopLeftX && point.x <= windowBottomRightX && point.y >= windowTopLeftY && point.y <= windowBottomRightY) {
                    context.write(new Text(grid.getId()), value);
                    break;
                }
            }
        }

        /**
         * Point class is used to represent the x and y coordinates of the data points being processed in the MapPoints class.
         * These coordinates are used to determine which grid the point belongs to and whether it is inside the specified window.
          */
        public static class Point {
            private final int x;
            private final int y;

            public Point(int x, int y) {
                this.x = x;
                this.y = y;
            }

            public int getX() {
                return x;
            }

            public int getY() {
                return y;
            }

            public static Point fromString(String str) {
                String[] parts = str.split(",");
                int x = Integer.parseInt(parts[0]);
                int y = Integer.parseInt(parts[1]);
                return new Point(x, y);
            }
        }

        /**
         * This class represents a rectangular grid in a two-dimensional coordinate system.
         * Each grid is identified by a unique ID string and defined by the coordinates of its
         * top-left and bottom-right corners.
         * It provides a method to check whether a given point is contained within the grid.
         */
        public static class Grid {
            private final String id;
            private final int x1;
            private final int y1;
            private final int x2;
            private final int y2;

            public Grid(String id, int x1, int y1, int x2, int y2) {
                this.id = id;
                this.x1 = x1;
                this.y1 = y1;
                this.x2 = x2;
                this.y2 = y2;
            }

            public String getId() {
                return id;
            }

            public boolean contains(Point point) {
                int x = point.getX();
                int y = point.getY();
                return (x >= x1 && x <= x2 && y >= y1 && y <= y2);
            }
        }


    }
//-------------------------------------------------------------------------------------------------------------------
    /**
     * MapRectangles class extends the Mapper class and is responsible for mapping input key/value pairs
     * to intermediate key/value pairs
     */
    public static class MapRectangles extends Mapper<Object, Text, Text, Text> {

        private Window window;
        private List<String> gridPartitions;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Fetch window coordinates from configuration
            Configuration conf = context.getConfiguration();
            String windowString = conf.get("window");
            window = Window.parseWindowString(windowString);

            // Divide input space into grid partitions
            gridPartitions = DivideIntoBlocks.createBlocks(10000, 2500);
        }

        /**
         * Processes a single input record representing a rectangle, and outputs the rectangle
         * to all grid partitions that intersect it. The grid partitions are determined in advance
         * by dividing the input space into fixed-size blocks. If a non-null window is specified in
         * the configuration, only rectangles that fall within the window are emitted.
         *
         * @param key     the input record key (unused)
         * @param value   the input record value, in the format "id,x,y,h,w"
         * @param context the Hadoop context object for emitting output
         */
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            int x1 = Integer.parseInt(fields[1]);
            int y1 = Integer.parseInt(fields[2]);
            int h = Integer.parseInt(fields[3]);
            int w = Integer.parseInt(fields[4]);
            int x2 = x1 + w;
            int y3 = y1 + h;

            int minX = Math.min(x1, x2);
            int maxX = Math.max(x1, x2);
            int minY = Math.min(y1, y3);
            int maxY = Math.max(y1, y3);

            for (String gridPartition : gridPartitions) {
                String[] gridDef = gridPartition.split(",");
                String gridNum = gridDef[0];
                int gridX1 = Integer.parseInt(gridDef[1]);
                int gridY1 = Integer.parseInt(gridDef[2]);
                int gridX2 = Integer.parseInt(gridDef[3]);
                int gridY2 = Integer.parseInt(gridDef[4]);

                if ((maxX >= gridX1) && (minX <= gridX2) && (maxY >= gridY1) && (minY <= gridY2)) {
                    if (window != null) {
                        if ((maxX >= window.x1) && (minX <= window.x2) && (maxY >= window.y1) && (minY <= window.y2)) {
                            String rectangleCoordinatess = fields[0] + "," + x1 + "," + y1 + "," + x2 + "," + y3;
                            context.write(new Text(gridNum), new Text(rectangleCoordinatess));
                        }
                    } else {
                        String rectangleCoords = fields[0] + "," + x1 + "," + y1 + "," + x2 + "," + y3;
                        context.write(new Text(gridNum), new Text(rectangleCoords));
                    }
                }
            }

        }

        /**
         * Window a helper class used to represent a rectangular window in the coordinate space.
         * It has four fields representing the coordinates of the top-left corner (x1,y1)
         * and the bottom-right corner (x2,y2) of the rectangular window.
         */
        public static class Window {
            private final int x1;
            private final int y1;
            private final int x2;
            private final int y2;

            public Window(int x1, int y1, int x2, int y2) {
                this.x1 = x1;
                this.y1 = y1;
                this.x2 = x2;
                this.y2 = y2;
            }

            /**
             *
             parses a string representation of the window coordinates passed as a configuration parameter.
             */
            public static Window parseWindowString(String windowString) {
                int x1 = 0;
                int y1 = 0;
                int x2 = 0;
                int y2 = 0;

                if (windowString != null && !windowString.equals("")) {
                    String[] windowAtt = windowString.split(",");
                    x1 = Integer.parseInt(windowAtt[0]);
                    y1 = Integer.parseInt(windowAtt[1]);
                    x2 = Integer.parseInt(windowAtt[2]);
                    y2 = Integer.parseInt(windowAtt[3]);
                }

                return new Window(x1, y1, x2, y2);
            }
        }

    }
}
