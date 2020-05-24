package network.popularity;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

public class CommentMapper extends Mapper<LongWritable, Text, Text, PostWritable> {
    private Text _parentId = new Text();
    private PostWritable _comment = new PostWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String line = value.toString();
        // Escaping pipe regex character.
        String[] words=line.split(Pattern.quote("|"));

        int dateColumn = 0;
        int commentColumn = 3;
        int parentColumn = 5;

        String dateCreated = words[dateColumn];
        String comment = words[commentColumn];

        _comment.setPost(comment);
        _comment.setCreationDate(dateCreated);

        String parentId =  words[parentColumn].length() != 0 ? words[parentColumn] : words[parentColumn + 1];
        _parentId.set(parentId);

        context.write(_parentId,_comment);
    }
}
