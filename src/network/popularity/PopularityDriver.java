package network.popularity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PopularityDriver extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityDriver(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration c=new Configuration();
        Job job=new Job(c,"popularity");

        // Paths
        Path p1 = new Path(args[0]); // posts
        Path p2 = new Path(args[1]); // comments
        Path p3 = new Path(args[2]); // output
        Path p4 = new Path(args[3]); // part-r-00000
        Path p5 = new Path(args[4]); // top-output


        FileSystem fs = FileSystem.get(c);
        if (fs.exists(p3)) {
            fs.delete(p3, true);
        }

        // Jar class
        job.setJarByClass(PopularityDriver.class);

        // Reducers
        job.setReducerClass(PopularityReducer.class);

        // Key values
        job.setOutputKeyClass(Text.class);

        // Mapper output value is different from the reduce output value.
        // In case we wanted to have the same output value classes
        // we'd use job.setOutputValueClass(<classname>.class)
        job.setMapOutputValueClass(PostWritable.class);
        job.setOutputValueClass(PostWritable.class);

        // Mappers
        MultipleInputs.addInputPath(job, p1, TextInputFormat.class, PostMapper.class);
        MultipleInputs.addInputPath(job, p2, TextInputFormat.class, CommentMapper.class);

        // Paths
        FileOutputFormat.setOutputPath(job, p3);

        job.waitForCompletion(true);

        Configuration topConf=new Configuration();
        Job topJob = new Job(topConf, "top");

        if (fs.exists(p5)) {
            fs.delete(p5, true);
        }

        topJob.setJarByClass(PopularityDriver.class);
        topJob.setMapperClass(TopMapper.class);
        topJob.setReducerClass(TopReducer.class);

        topJob.setOutputKeyClass(IntWritable.class);
        topJob.setOutputValueClass(TopWritable.class);

        FileInputFormat.addInputPath(topJob, p4);
        FileOutputFormat.setOutputPath(topJob, p5);

        topJob.waitForCompletion(true);
        return 0;

    }
}
