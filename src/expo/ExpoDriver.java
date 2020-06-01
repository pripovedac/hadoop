package expo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FilePermission;
import java.io.IOException;

public class ExpoDriver extends Configured implements Tool{

    public static void main (String [] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new ExpoDriver(), args));
    }

    public int run (String [] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration c = new Configuration();
        Job job = new Job(c, "expo");

        Path p1 = new Path(args[0]);
        Path p2 = new Path(args[1]);

        FileSystem fs = FileSystem.get(c) ;
        if(fs.exists(p2)) {
            fs.delete(p2);
        }

        job.setJarByClass(ExpoDriver.class);
        job.setMapperClass(ExpoMapper.class);
        job.setReducerClass(ExpoReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, p1);
        FileOutputFormat.setOutputPath(job, p2);

        job.waitForCompletion(true);
        return 0;

    }
}
