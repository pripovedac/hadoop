package network.popularity;

import java.io.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopReducer extends Reducer<IntWritable, TopWritable, IntWritable, TopWritable> {

    static int count;
    private TopWritable _top = new TopWritable();


    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        count = 0;
    }

    @Override
    public void reduce(IntWritable key, Iterable<TopWritable> values, Context context) throws IOException, InterruptedException {

        int reps = (-1) * key.get();

        for (TopWritable value : values) {
            _top = value;
        }

        // We just write 3 records as output.
        if (count < 3) {
            context.write(new IntWritable(reps), _top);
            count++;
        }
    }
}