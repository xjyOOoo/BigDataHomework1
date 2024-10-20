package com.example.hadoop;

import java.io.IOException;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StockCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Text stockCode = new Text();
    private final static LongWritable one = new LongWritable(1);
    private CSVParser csvParser;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        csvParser = new CSVParserBuilder()
                .withSeparator(',')
                .withQuoteChar('"')
                .build();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = csvParser.parseLine(line);

        if (fields.length >= 4) {
            String stock = fields[3].trim();
            if (!stock.isEmpty()) {
                stockCode.set(stock);
                context.write(stockCode, one);
            }
        }
    }
}