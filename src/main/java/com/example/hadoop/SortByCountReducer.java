package com.example.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortByCountReducer extends Reducer<LongWritable, Text, Text, LongWritable> {
    private long sequenceNumber = 0;

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text val : values) {
            Text outputKey = new Text(sequenceNumber + "\t" + val.toString());
            context.write(outputKey, key);
            sequenceNumber++;
        }
    }
}