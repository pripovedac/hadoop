package partitioner;

import network.average.AveragePartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.ToolRunner;
import wordcount.WordDriver;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class PartDriver extends Configured implements Tool {

    public static void main (String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new PartDriver(), args));
    }

    public int run(String[] args) throws Exception {
        Job job = new Job(new Configuration(), "partitioner");

        System.out.println("Inside PartDriver.");


        job.setJarByClass(PartDriver.class);
        job.setMapperClass(PartMapper.class);
        job.setReducerClass(PartReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Partitioner
        job.setPartitionerClass(PartPartitioner.class);
        job.setNumReduceTasks(2);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path((args[1])));

        job.waitForCompletion(true);
        return 0;
    }
}
