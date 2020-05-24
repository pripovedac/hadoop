package network.popularity;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TopWritable implements Writable {
    private Text _postId;
    private Text _post;

    public TopWritable() {
        _post = new Text("");
        _postId = new Text("");
    }

    public TopWritable(Text postId, Text post) {
        this._post = post;
        this._postId = postId;
    }

    public Text getPost() {
        return _post;
    }

    public Text getPostId() {
        return _postId;
    }

    public void setPost(String post) {
        this._post = new Text(post);
    }

    public void setPostId(String postId) {
        this._postId =  new Text(postId);
    }

    public void readFields(DataInput in) throws IOException {
        _post.readFields(in);
        _postId.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        _post.write(out);
        _postId.write(out);
    }

    @Override
    public String toString() {
        return  _post.toString() + "\t" + _postId.toString();
    }

}