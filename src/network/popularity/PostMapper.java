package network.popularity;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.regex.Pattern;

public class PostMapper extends Mapper<LongWritable, Text, Text, PostWritable> {
    private Text _parentId = new Text();
    private PostWritable _post = new PostWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String line = value.toString();

        // Escaping pipe regex character.
        String[] words = line.split(Pattern.quote("|"));

        int idColumn = 1;
        int dateCreatedColumn = 0;
        int postColumn = 3;

        // key
        String parentId =  words[idColumn];
        _parentId.set(parentId);

        // value
        String dateCreated = words[dateCreatedColumn];
        String post = words[postColumn];

        _post.setCreationDate(dateCreated);
        _post.setPost(post);

        context.write(_parentId, _post);
    }
}