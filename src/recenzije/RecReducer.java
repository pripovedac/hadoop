package recenzije;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.*;

public class RecReducer extends Reducer<IntWritable, Text, Text, Text>{
    private Integer _min = 100;
    private Integer _versionMin = -1;

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        for(Text value : values) {
            String[] tokens = value.toString().split(" ");
            Integer version = new Integer(tokens[0]);
            Integer mark = new Integer(tokens[1]);

            if (mark < _min) {
                _min = mark;
                _versionMin = version;
            }
        }

        Text outputKey = new Text("Year: " + Integer.toString(key.get()));
        Text outputValue = new Text("Version: " + Integer.toString(_versionMin) + ", Mark: " + Integer.toString(_min));

        ctx.write(outputKey, outputValue);

    }
}
