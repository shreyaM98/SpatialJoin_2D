import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SpatialReducer {

/**
 Reducer class  receives a key and a list of values (points or rectangles - comma separated strings) from a mapper.
 */
    public static class SpatialJoinReduce extends Reducer<Text, Text, Text, Text> {

    /**
    reduce extracts the points and rectangles from the values list and checks if each point lies inside any of the rectangles.
    It emits the rectangle ID and the point coordinates as the key-value pair for the output.

    @param key The key received by the reducer.
    @param values The list of values received by the reducer.
    @param context The context object for writing the output key-value pairs.
*/
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Point> pointList = new ArrayList<>();
            List<Rectangle> rectangleList = new ArrayList<>();
            for(Text value: values){
                String[] valueArr = value.toString().split(",");
                if(valueArr.length==2){
                    int pointX = Integer.parseInt(valueArr[0]);
                    int pointY = Integer.parseInt(valueArr[1]);
                    pointList.add(new Point(pointX, pointY));
                }
                else{
                    int recX1 = Integer.parseInt(valueArr[1]);
                    int recY1 = Integer.parseInt(valueArr[2]);
                    int recX2 = Integer.parseInt(valueArr[3]);
                    int recY2 = Integer.parseInt(valueArr[4]);
                    rectangleList.add(new Rectangle(valueArr[0], recX1, recY1, recX2, recY2));
                }
            }
            for(Point point:pointList){
                Set<String> overlappingRectangleIds = new HashSet<>();
                for(Rectangle rectangle:rectangleList){
                    if(rectangle.contains(point)){
                        overlappingRectangleIds.add(rectangle.getId());
                    }
                }
                for(String overlappingRectangleId : overlappingRectangleIds) {
                    String resultVal = point.toString();
                    context.write(new Text(overlappingRectangleId), new Text(resultVal));
                }
            }
        }
        }

    /**
     * Point class represents a 2D point with x and y coordinates.
     */
    public static class Point {
            private final int x;
            private final int y;

            public Point(int x, int y){
                this.x = x;
                this.y = y;
            }

            public int getX() {
                return x;
            }

            public int getY() {
                return y;
            }

            @Override
            public String toString() {
                return "(" + x + "," + y + ")";
            }
        }

    /**
     This is a Rectangle class that represents a rectangle with x and y coordinates of two opposite corners.
     It also has a unique ID to identify the rectangle.
     */

        public static class Rectangle {
            private final String id;
            private final int x1;
            private final int y1;
            private final int x2;
            private final int y2;

            public Rectangle(String id, int x1, int y1, int x2, int y2){
                this.id = id;
                this.x1 = x1;
                this.y1 = y1;
                this.x2 = x2;
                this.y2 = y2;
            }

            public String getId() {
                return id;
            }

            public boolean contains(Point point){
                int pointX = point.getX();
                int pointY = point.getY();
                return (pointX >= x1) && (pointX <= x2) && (pointY >= y1) && (pointY <= y2);
            }
        }
    }

}