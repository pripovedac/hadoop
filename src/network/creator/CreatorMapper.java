package network.creator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

public class CreatorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable ONE = new IntWritable(1);
    private Text _key = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // Escaping pipe regex character.
        String[] words = line.split(Pattern.quote("|"));

        int idColumn = 2;
        int creatorColumn = 4;

        String creatorId = words[idColumn];
        String creator = words[creatorColumn];

        _key.set(creatorId + " " + creator);

        context.write(_key, ONE);
    }
}
