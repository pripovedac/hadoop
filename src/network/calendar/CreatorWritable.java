package network.calendar;

import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CreatorWritable implements Writable {
    private Text _creator;
    private Text _creatorId;
    private IntWritable _max;

    public CreatorWritable() {
        _creator= new Text("");
        _creatorId = new Text("");
        _max = new IntWritable(0);
    }

    public CreatorWritable(Text creator, Text creatorId) {
        _creator= new Text(creator);
        _creatorId = new Text(creatorId);
        _max = new IntWritable(0);
    }

    public Text getCreator() {
        return _creator;
    }

    public Text getCreatorId() {
        return _creatorId;
    }

    public IntWritable getCreatorMax() {
        return _max;
    }

    public void setCreator(Text creator) {
        this._creator = creator;
    }

    public void setCreatorId(Text creatorId) {
        this._creatorId = creatorId;
    }

    public void setMax(IntWritable max) {
        this._max= max;
    }

    public void readFields(DataInput in) throws IOException {
        _creator.readFields(in);
        _creatorId.readFields(in);
        _max.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        _creator.write(out);
        _creatorId.write(out);
        _max.write(out);
    }

    @Override
    public String toString() {
        return _creatorId.toString() + "\t" + _creator.toString() + "\t" + _max.toString();
    }

}