
package network.popularity;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class TopMapper extends Mapper<LongWritable, Text, IntWritable, TopWritable> {
    private Text _text = new Text();
    private IntWritable _reps = new IntWritable();
    private TopWritable _top = new TopWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // Escaping tab character.
        String[] words = line.split("\t");

        // key
        int repsColumn = 2;
        int reps = Integer.parseInt(words[repsColumn]) * (-1);

        _reps.set(reps);

        int textColumn = 1;
        int idColumn = 0;

        // value
        String text = words[textColumn];
        _text.set(text);

        String postId = words[idColumn];

        _top.setPost(text);
        _top.setPostId(postId);

        context.write(_reps, _top);
    }
}