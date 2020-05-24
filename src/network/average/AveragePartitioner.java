package network.average;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class AveragePartitioner extends Partitioner<IntWritable, IntWritable>  {

    @Override
    public int getPartition(IntWritable key, IntWritable value, int i) {
        return key.get() - 1;
    }
}
