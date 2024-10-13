package com.example.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Set<String> stopWords = new HashSet<>();
    private Text wordText = new Text();
    private final static LongWritable one = new LongWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String stopWordsPath = context.getConfiguration().get("stopwords.path");
        if (stopWordsPath != null) {
            Path path = new Path(stopWordsPath);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream in = fs.open(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = br.readLine()) != null) {
                stopWords.add(line.trim().toLowerCase());
            }
            br.close();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //CSV格式: <索引>,<标题>,<发布时间戳记>,<股票代码>
        String line = value.toString();
        String[] fields = line.split(",", -1);
        if (fields.length == 4) {
            String headline = fields[1].trim();
            if (!headline.isEmpty()) {
                //去除标点符号，转换为小写
                String cleaned = headline.replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();
                String[] words = cleaned.split("\\s+");
                for (String word : words) {
                    if (!word.isEmpty() && !stopWords.contains(word)) {
                        wordText.set(word);
                        context.write(wordText, one);
                    }
                }
            }
        }
    }
}