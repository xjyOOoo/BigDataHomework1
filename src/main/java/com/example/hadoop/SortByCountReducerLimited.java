package com.example.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortByCountReducerLimited extends Reducer<LongWritable, Text, Text, LongWritable> {
    private int count = 0;
    private static final int TOP_N = 100;
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text val : values) {
            if (count<TOP_N){
                context.write(val, key);
                count++;
            }else{
                break;
            }
        }
    }
}