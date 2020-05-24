package anagram;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnagramMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {

        String word = value.toString();

        char[] wordChars = word.toCharArray();
        Arrays.sort(wordChars);

        String sortedWord = new String(wordChars);

        ctx.write(new Text(sortedWord), new Text(word));

    }



}
