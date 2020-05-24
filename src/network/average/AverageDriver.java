package network.average;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new AverageDriver(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        System.out.println("Cao, Darenceeee! Sada radimo partitionera. :D");
        Configuration c = new Configuration();
        Job job = new Job(c, "average");

        // Paths
        Path p1 = new Path(args[0]); // posts
        Path p2 = new Path(args[1]); // comments
        Path p3 = new Path(args[2]); // output

        FileSystem fs = FileSystem.get(c);
        if (fs.exists(p3)) {
            fs.delete(p3, true);
        }

        // Jar class
        job.setJarByClass(AverageDriver.class);

        // Mappers
        MultipleInputs.addInputPath(job, p1, TextInputFormat.class, AverageMapper.class);
        MultipleInputs.addInputPath(job, p2, TextInputFormat.class, AverageMapper.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Reducers
        job.setReducerClass(AverageReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        // Partitioner
        job.setPartitionerClass(AveragePartitioner.class);
        job.setNumReduceTasks(12);

        // Paths
        FileOutputFormat.setOutputPath(job, p3);

        job.waitForCompletion(true);
        return 0;
    }
}
