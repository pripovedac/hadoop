package network.creator;


import com.sun.source.tree.Tree;
import network.popularity.PostWritable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class CreatorReducer extends Reducer <Text, IntWritable, Text, IntWritable> {

    private TreeMap<Integer, String> _tree;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        _tree = new TreeMap<Integer,String> ();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            sum *= (-1);
            _tree.put(sum, key.toString());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        for(Map.Entry<Integer, String> entry : _tree.entrySet()) {
            int count = entry.getKey() * (-1);
            String creator = entry.getValue();
            context.write(new Text(creator),  new IntWritable(count));
        }
    }
}
