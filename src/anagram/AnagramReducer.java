package anagram;

import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.Text;

public class AnagramReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String anagrams = "";

        for(Text word : values) {
            anagrams += word;
            anagrams += " ";
        }

        context.write(key, new Text(anagrams));
    }
}