package recenzije;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RecMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        // godina, autor, verzija, ocena
        String[] tokens = value.toString().split(" ");

        Integer _key = new Integer(tokens[0]);
        String _value = tokens[2] + " " + tokens[3];

        ctx.write(new IntWritable(_key), new Text(_value));
    }
}
