import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Spatial2dJoin {

    public static void main(String[] args) throws Exception {
        Spatial2dJoin spatial2dJoin = new Spatial2dJoin();
        spatial2dJoin.debugSpatial2dJoin(args);
    }

    public void debugSpatial2dJoin (String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        // Set the window configuration parameter
        conf.set("window",args[3]);

        // Instantiate a new MapReduce job object
        Job job = Job.getInstance(conf, "Spatial2dJoin");

        // Delete output directory if it already exists
        FileSystem.get(conf).delete(new Path(args[2]), true);

        // job attributes
        job.setJarByClass(Spatial2dJoin.class);
        job.setJobName("Spatial2dJoin");
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(SpatialReducer.SpatialJoinReduce.class);

        // input paths for points and rectangles files
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, SpatialMapper.MapPoints.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SpatialMapper.MapRectangles.class);

        // Set output path
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }

}
