import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SpatialJoin {

    public static List<String> divideIntoBlocks(Integer maxPoint, Integer blockSize) {
        List<String> blocks = new ArrayList<>();
        int blockCount = 1;
        for (int i = 0; i < maxPoint; i = i + blockSize) {
            for (int j = 0; j < maxPoint; j = j + blockSize) {
                String block = "g";
                int x1 = i;
                int y1 = j;
                int x2 = x1+blockSize;
                int y2 = y1+blockSize;
                block = block + String.valueOf(blockCount) + "," + String.valueOf(x1) + "," + String.valueOf(y1) + "," + String.valueOf(x2)
                        + "," + String.valueOf(y2);
                blocks.add(block);
                blockCount++;
            }
        }
        return blocks;
    }

    public static class PointMap extends Mapper<Object, Text, Text, Text> {

        private String spatialWindow;

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            spatialWindow = conf.get("spatialWindow");
        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            List<String> blockPartitions = divideIntoBlocks(10000,2500);

            String[] windowCoordinates = new String[4];

            if(spatialWindow != null && !spatialWindow.equals("")) {
                windowCoordinates = spatialWindow.split(",");
            }

            String[] eachPoint = value.toString().split(",");
            int x = Integer.parseInt(eachPoint[0]);
            int y = Integer.parseInt(eachPoint[1]);

            for(int i=0; i<blockPartitions.size(); i++) {
                String[] blockDef = blockPartitions.get(i).split(",");
                String blockNum = blockDef[0];
                if ((x >= Integer.parseInt(blockDef[1])) && (x <= Integer.parseInt(blockDef[3])) && (y >= Integer.parseInt(blockDef[2]))
                        && (y <= Integer.parseInt(blockDef[4]))){
                    if(spatialWindow != null && !spatialWindow.equals("")){
                        if((x >= Integer.parseInt(windowCoordinates[0])) && (x <= Integer.parseInt(windowCoordinates[2])) &&
                                (y >= Integer.parseInt(windowCoordinates[1])) && (y <= Integer.parseInt(windowCoordinates[3]))){
                            context.write(new Text(blockNum), value);
                            break;
                        }
                    }
                    else{
                        context.write(new Text(blockNum), value);
                        break;
                    }
                }
            }
        }
    }

    public static class RectangleMap extends Mapper<Object, Text, Text, Text> {

        private String spatialWindow;

        public void setup(Context context)  {
            Configuration conf = context.getConfiguration();
            spatialWindow = conf.get("spatialWindow");
        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            List<String> blockPartitions = divideIntoBlocks(10000,2500);

            int[] integerWindow = new int[4];
            if(spatialWindow != null && spatialWindow != "") {
                String[] windowCoordinates = spatialWindow.split(",");
                int j = 0;
                while (j < 4) {
                    integerWindow[j] = Integer.parseInt(windowCoordinates[j]);
                    j++;
                }
            }

            String[] rectangle = value.toString().split(",");

            int x1 = Integer.parseInt(rectangle[1]);
            int y1 = Integer.parseInt(rectangle[2]);
            int h = Integer.parseInt(rectangle[3]);
            int w = Integer.parseInt(rectangle[4]);
            int x2 = x1+w;
            int y2 = y1;
            int x3 = x1;
            int y3 = y1+h;
            int x4 = x2;
            int y4 = y3;


            for(int i=0;i<blockPartitions.size();i++) {
                String[] blockCoord = blockPartitions.get(i).split(",");
                int[] integerBlock = new int[5];
                int j = 1;
                while(j <= 4){
                    integerBlock[j] = Integer.parseInt(blockCoord[j]);
                    j++;
                }

                if (((x1 >= integerBlock[1]) && (x1 <= integerBlock[3]) && (y1 >= integerBlock[2]) && (y1 <= integerBlock[4])) ||
                        ((x2 >= integerBlock[1]) && (x2 <= integerBlock[3]) && (y2 >= integerBlock[2]) && (y2 <= integerBlock[4])) ||
                        ((x3 >= integerBlock[1]) && (x3 <= integerBlock[3]) && (y3 >= integerBlock[2]) && (y3 <= integerBlock[4])) ||
                        ((x4 >= integerBlock[1]) && (x4 <= integerBlock[3]) && (y4 >= integerBlock[2]) && (y4 <= integerBlock[4])))
                {
                    if(spatialWindow != null && spatialWindow != "")
                    {
                        if(((x1 >= integerWindow[0]) && (x1 <= integerWindow[2]) && (y1 >= integerWindow[1]) && (y1 <= integerWindow[3])) ||
                                ((x2 >= integerWindow[0]) && (x2 <= integerWindow[2]) && (y2 >= integerWindow[1]) && (y2 <= integerWindow[3])) ||
                                ((x3 >= integerWindow[0]) && (x3 <= integerWindow[2]) && (y3 >= integerWindow[1]) && (y3 <= integerWindow[3])) ||
                                ((x4 >= integerWindow[0]) && (x4 <= integerWindow[2]) && (y4 >= integerWindow[1]) && (y4 <= integerWindow[3]))) {
                            String rectangleCoords = rectangle[0] + "," + String.valueOf(x1) + "," + String.valueOf(y1) + "," + String.valueOf(x4) + "," + String.valueOf(y4);
                            context.write(new Text(blockCoord[0]), new Text(rectangleCoords));
                        }
                    }
                    else{
                        String rectangleCoords = rectangle[0] + "," + String.valueOf(x1) + "," + String.valueOf(y1) + "," + String.valueOf(x4) + "," + String.valueOf(y4);
                        context.write(new Text(blockCoord[0]), new Text(rectangleCoords));
                    }
                }
            }
        }
    }

    public static class SpatialJoinReducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> pointList = new ArrayList<>();
            List<String> rectangleList = new ArrayList<>();
            for(Text value: values){
                String[] listOfValue = value.toString().split(",");
                if(listOfValue.length == 2){
                    pointList.add(value.toString());
                }
                else{
                    rectangleList.add(value.toString());
                }
            }
            for(String point:pointList){
                String[] pCoordinates = point.split(",");
                for(String rectangle:rectangleList){
                    String[] rCoordinates = rectangle.split(",");
                    if ((Integer.parseInt(pCoordinates[0]) >= Integer.parseInt(rCoordinates[1])) &&
                            (Integer.parseInt(pCoordinates[0]) <= Integer.parseInt(rCoordinates[3])) &&
                            (Integer.parseInt(pCoordinates[1]) >= Integer.parseInt(rCoordinates[2])) &&
                            (Integer.parseInt(pCoordinates[1]) <= Integer.parseInt(rCoordinates[4]))) {
                        String resultVal = "<"+pCoordinates[0]+","+pCoordinates[1]+">";
                        context.write(new Text(rCoordinates[0]), new Text(resultVal));
                    }
                }
            }
        }
    }

    public static void main(String [] args) throws Exception {

        Configuration configuration = new Configuration();
        FileSystem.get(configuration).delete(new Path(args[2]), true);

        configuration.set("spatialWindow", args[3]);

        Job job = Job.getInstance(configuration, "SpatialJoin");
        job.setJarByClass(SpatialJoin.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(SpatialJoin.SpatialJoinReducer.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PointMap.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RectangleMap.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
