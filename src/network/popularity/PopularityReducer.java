package network.popularity;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class PopularityReducer extends Reducer <Text, PostWritable, Text, PostWritable> {

    private Text _key;
    private PostWritable _output = new PostWritable();
    private SimpleDateFormat _dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    protected void reduce(Text key, Iterable<PostWritable> values, Context context) throws IOException, InterruptedException {
        {
            _key = key;

            List<PostWritable> maxBuffer = new ArrayList<PostWritable>();
            List<PostWritable> diffBuffer = new ArrayList<PostWritable>();

            for (PostWritable value : values) {
                maxBuffer.add(new PostWritable(value.getCreationDate(), value.getPost()));
                diffBuffer.add(new PostWritable(value.getCreationDate(), value.getPost()));
            }

            // Year 2100 is pseudomaximum.
            Date parentDate = null;

            try {
                parentDate = _dateFormat.parse("2100-01-01");
            } catch (ParseException e) {
                e.printStackTrace();
            }

            for (PostWritable value : maxBuffer ) {
                Date date = null;
                try {
                    date = _dateFormat.parse(value.getCreationDate().toString());
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                if (date.compareTo(parentDate) < 0) {
                    parentDate = date;
                    _output.setCreationDate( date.toString());
                    _output.setPost(value.getPost().toString());
                }
            }

            int sum = 0;

            for (PostWritable value : diffBuffer) {
                long diff = 0;
                try {
                    diff = parentDate.getTime() - _dateFormat.parse(value.getCreationDate().toString()).getTime();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                long days = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
                sum += days >= 10 ?  0 : 10 - days;
            }

            // Every post calculated itself.
            sum -= 10;

            _output.setPopularity(sum);

            context.write(_key, _output);
        }
    }
}
