package expo;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ExpoMapper extends Mapper<LongWritable, Text, Text, Text>{
    private SimpleDateFormat _dateFormater = new SimpleDateFormat("yyyy-MM-dd");
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Europe/Paris"));

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(" ");
        String inputDate = tokens[0];
        String pavillonId = tokens[1];
        Integer direction = Integer.parseInt(tokens[3]);

        Date startingDate = null;
        Date endingDate = null;
        Date convertedDate = null;

        try {
            startingDate = _dateFormater.parse("2018-07-01");
            endingDate = _dateFormater.parse("2018-07-31");
            convertedDate = _dateFormater.parse(inputDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if (convertedDate.compareTo(startingDate) >= 0 && convertedDate.compareTo(endingDate) <= 0 && direction == 1) {
           cal.setTime(convertedDate);

            String parsedDate = Integer.toString(cal.get(Calendar.DAY_OF_MONTH));
            String outputValue = parsedDate + " " + pavillonId + " ";

            ctx.write(new Text("expo"), new Text(outputValue));
        }


    }

}
