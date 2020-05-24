package network.popularity;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PostWritable implements Writable {
    private Text _post;
    private Text _creationDate;
    private IntWritable _popularity;

    public PostWritable() {
        _post = new Text("");
        _creationDate = new Text("");
        _popularity = new IntWritable(0);
    }

    public PostWritable(Text creationDate, Text post) {
        this._post = post;
        this._creationDate = creationDate;
        _popularity = new IntWritable(0);
    }

    public Text getCreationDate() {
        return _creationDate;
    }

    public Text getPost() {
        return _post;
    }

    public IntWritable getPopularity() {
        return _popularity;
    }

    public void setPost(String post) {
        this._post = new Text(post);
    }

    public void setCreationDate(String creationDate) {
        this._creationDate = new Text(creationDate);
    }

    public void setPopularity(int popularity) {
        this._popularity =  new IntWritable(popularity);
    }

    public void readFields(DataInput in) throws IOException {
        _post.readFields(in);
        _creationDate.readFields(in);
        _popularity.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        _post.write(out);
        _creationDate.write(out);
        _popularity.write(out);
    }

    @Override
    public String toString() {
        return  _post.toString() + "\t" + _popularity.toString();
    }

}