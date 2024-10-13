package com.example.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StockCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Text stockCode = new Text();
    private final static LongWritable one = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //CSV格式: <索引>,<标题>,<发布时间戳记>,<股票代码>
        String line = value.toString();
        String[] fields = line.split(",", -1);

        if (fields.length == 4) {
            String stock = fields[3].trim();
            if (!stock.isEmpty()) {
                stockCode.set(stock);
                context.write(stockCode, one);
            }
        }
    }
}