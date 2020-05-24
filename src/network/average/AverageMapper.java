package network.average;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

public class AverageMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    private IntWritable _key;
    private IntWritable _value;
    private SimpleDateFormat _dateFormat;
    private SimpleDateFormat _monthFormat;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        _key = new IntWritable();
        _value = new IntWritable();
        _dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        _monthFormat = new SimpleDateFormat("MM");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // Escaping pipe regex character.
        String[] words = line.split(Pattern.quote("|"));

        int dateColumn = 0;
        String date = words[dateColumn];
        Date startingDate = null;
        Date endingDate = null;
        Date convertedDate = null;
        try {
            startingDate = _dateFormat.parse("2010-01-01");
            endingDate = _dateFormat.parse("2010-12-31");
            convertedDate = _dateFormat.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if (convertedDate.compareTo(startingDate) > 0 && convertedDate.compareTo(endingDate) < 0) {
            int month = Integer.parseInt(_monthFormat.format(convertedDate));

            int textColumn = 3;
            int textLength = words[textColumn].length();

            _key.set(month);
            _value.set(textLength);

            context.write(_key, _value);
        }
    }
}
