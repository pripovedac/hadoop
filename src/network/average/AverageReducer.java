package network.average;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;



public class AverageReducer extends Reducer<IntWritable, IntWritable, Text, FloatWritable> {

    private Text _key;
    private FloatWritable _avg;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        _key = new Text();
        _avg = new FloatWritable();
    }

    String getMonth(int month) {
        String monthString;
        switch (month) {
            case 1:  monthString = "January";
                break;
            case 2:  monthString = "February";
                break;
            case 3:  monthString = "March";
                break;
            case 4:  monthString = "April";
                break;
            case 5:  monthString = "May";
                break;
            case 6:  monthString = "June";
                break;
            case 7:  monthString = "July";
                break;
            case 8:  monthString = "August";
                break;
            case 9:  monthString = "September";
                break;
            case 10: monthString = "October";
                break;
            case 11: monthString = "November";
                break;
            case 12: monthString = "December";
                break;
            default: monthString = "Invalid month";
                break;
        }
        return monthString;
    }

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        {
            String month = getMonth(key.get());
            _key.set(new Text(month));

            Integer sum = 0;
            Integer size = 0;

            for (IntWritable value : values) {
                sum += value.get();
                size++;
            }

            _avg = new FloatWritable(sum / size);
            context.write(_key, _avg);
        }
    }



}
