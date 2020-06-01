package jan20;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.*;

public class Jan20Reducer extends Reducer<Text, Text, IntWritable, IntWritable> {
//    private Text key;
    private Hashtable<Integer, Integer> _hash = new Hashtable<Integer, Integer>();
    private Integer _max = 0;
    private Integer _maxId;

    @Override
    protected void reduce (Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        for (Text value : values) {
            String [] tokens = value.toString().split(" ");

            Integer id = new Integer(tokens[0]);
            Integer data = new Integer(tokens[1]);

            if (!_hash.containsKey(id)) {
                _hash.put(id, data);

                if (data > _max) {
                    _max = data;
                    _maxId = id;
                }
            } else {
                Integer oldValue = _hash.get(id);
                Integer newValue = oldValue + data;
                _hash.remove(id);
                _hash.put(id, newValue);

                if (newValue > _max) {
                    _max = newValue;
                    _maxId = id;
                }
            }
        }

        Set<Integer> keys = _hash.keySet();

        for (Integer myKey: keys) {
            ctx.write(new IntWritable(myKey),new IntWritable(_hash.get(myKey)));
        }

        _hash.clear();
    }

    @Override
    protected void cleanup(Context ctx) throws IOException, InterruptedException {
        ctx.write(new IntWritable(_maxId), new IntWritable(_max));
    }

}