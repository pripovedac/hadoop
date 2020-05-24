package network.calendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.*;


public class CalendarReducer extends Reducer<Text, CreatorWritable, Text, CreatorWritable> {

    private Text _key;
    private Hashtable<Text, Integer> _hash;
    private int _min;
    private CreatorWritable _creator;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        _key = new Text();
        _hash = new Hashtable<Text, Integer>();
        _min = 0;
        _creator = new CreatorWritable();
    }

    @Override
    protected void reduce(Text key, Iterable<CreatorWritable> values, Context context) throws IOException, InterruptedException {
        {
            _key.set(key);

            for (CreatorWritable creator : values) {

                if (!_hash.containsKey(creator.getCreatorId())) {
                    _hash.put(creator.getCreatorId(), new Integer(1));
                } else {
                    Integer oldValue = _hash.get(creator.getCreatorId());
                    _hash.remove(creator.getCreatorId());
                    _hash.put(creator.getCreatorId(), oldValue + 1);
                }

                Integer count = _hash.get(creator.getCreatorId());

                if (count > _min) {
                    _min = count;
                    _creator.setCreatorId(creator.getCreatorId());
                    _creator.setCreator(creator.getCreator());
                    _creator.setMax(new IntWritable(count));
                }
            }
            _hash.clear();
            context.write(_key, _creator);
        }
    }

}
