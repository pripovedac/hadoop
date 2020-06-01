package recenzije;

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

import java.io.IOException;

public class RecDriver extends Configured implements Tool {

    public static void main(String [] args) throws Exception{
        System.exit(ToolRunner.run(new Configuration(), new RecDriver(), args));
    }

    public int run (String [] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration c = new Configuration();
        Job job = new Job(c, "rec");

        Path p1 = new Path(args[0]);
        Path p2 = new Path(args[1]);

        FileSystem fs = FileSystem.get(c);
        if(fs.exists(p2)) {
            fs.delete(p2, true);
        }

        job.setJarByClass(RecDriver.class);
        job.setMapperClass(RecMapper.class);
        job.setReducerClass(RecReducer.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(IntWritable.class);

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);

        FileInputFormat.addInputPath(job, p1);
        FileOutputFormat.setOutputPath(job, p2);

        job.waitForCompletion(true);
        return 0;
    }
}
