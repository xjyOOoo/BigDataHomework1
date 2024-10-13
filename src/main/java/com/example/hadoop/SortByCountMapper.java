package com.example.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortByCountMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private LongWritable count = new LongWritable();
    private Text keyText = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //input format: <Key>\t<Value>
        String[] parts = value.toString().split("\t");
        if (parts.length == 2) {
            String keyStr = parts[0].trim();
            try {
                long cnt = Long.parseLong(parts[1].trim());
                count.set(cnt);
                keyText.set(keyStr);
                context.write(count, keyText);
            } catch (NumberFormatException e) {
            }
        }
    }
}