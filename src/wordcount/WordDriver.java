package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordDriver extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordDriver(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        System.out.println("Cao, Darenceeee!");
        Configuration c=new Configuration();
        Job job=new Job(c,"wordcount");

        job.setJarByClass(WordDriver.class);
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        return 0;

    }
}
