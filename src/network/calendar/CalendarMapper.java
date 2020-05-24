package network.calendar;

import org.apache.commons.net.imap.IMAPClient;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

public class CalendarMapper extends Mapper<LongWritable, Text, Text, CreatorWritable> {
    private CreatorWritable _creator;
    private Text _key;
    private SimpleDateFormat _dateFormat;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        _creator = new CreatorWritable();
        _key = new Text();
        _dateFormat = new SimpleDateFormat("yyyy-MM-dd");
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
            startingDate = _dateFormat.parse("2010-02-01");
            endingDate = _dateFormat.parse("2010-02-28");
            convertedDate = _dateFormat.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if (convertedDate.compareTo(startingDate) > 0 && convertedDate.compareTo(endingDate) < 0) {
            int idColumn = 2;
            int creatorColumn = 4;

            String creatorId = words[idColumn];
            String creator = words[creatorColumn];

            _key.set(new Text(convertedDate.toString()));
            _creator.setCreator(new Text(creator));
            _creator.setCreatorId(new Text(creatorId));

            context.write(_key, _creator);
        }
    }
}
