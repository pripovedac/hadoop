package expo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.*;

public class ExpoReducer extends Reducer<Text, Text, Text, Text> {
    private Hashtable<Integer, Integer> _hash = new Hashtable<Integer, Integer>();
    private Integer _max = 0;
    private Integer _maxPavillion = -1;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        for(Text value: values) {
            String[] tokens = value.toString().split(" ");
            Integer day = Integer.parseInt(tokens[0]);
            Integer pavillion = Integer.parseInt(tokens[1]);

            if (!_hash.containsKey(day)) {
                _hash.put(day, 1);

                if (_max < 1) {
                    _max = 1;
                    _maxPavillion = pavillion;
                }
            } else {
                Integer oldValue = _hash.get(day);
                Integer newValue = oldValue + 1;

                _hash.remove(day);
                _hash.put(day, oldValue + 1);

                if (_max < newValue) {
                    _max = newValue;
                    _maxPavillion = pavillion;
                }
            }
        }

        Set<Integer> keys = _hash.keySet();

        for(Integer _key : keys) {
            Text outputKey = new Text("Day: " + Integer.toString(_key));
            Text outputValue = new Text("Value: " + Integer.toString(_hash.get(_key)));

            ctx.write(outputKey, outputValue);
        }

        Text maxKey = new Text("Max: " + _max);
        Text maxOutput = new Text("Pavillion: " + _maxPavillion);

        ctx.write(maxKey, maxOutput);
    }
}
