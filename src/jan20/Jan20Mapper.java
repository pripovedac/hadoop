package jan20;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Jan20Mapper extends Mapper<LongWritable, Text, Text, Text> {


    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        String line = value.toString();

        //january 1 10 1000

        String [] tokens = line.split(" ");
        Text month = new Text(tokens[0]);
        Text reducerValue = new Text(tokens[1] + " " + tokens[3]);

        ctx.write(month, reducerValue);
    }

}