package partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.Text;


public class PartReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private int max = 0;
    private Text maxWord = new Text();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context ctx) {
        for (IntWritable value: values) {

            int intValue = value.get();

            if (intValue > max) {
                max = intValue;
                maxWord = key;
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        context.write(maxWord, new IntWritable(max));
    }
}
